import bisect
import copy
import os
import threading
import typing
import sc2gamedata
from bottle import route, run

_ACCESS_TOKEN = os.getenv('BATTLE_NET_ACCESS_TOKEN', "")
_REFRESH_INTERVAL = 30


class RepeatingTaskScheduler:

    def __init__(self, task: typing.Callable[[], None]):
        self.active_timer = None
        self.timer_lock = threading.Lock()
        self.task = task

    def schedule(self, delay: int):
        self.timer_lock.acquire()
        if not self.active_timer or not self.active_timer.is_alive():
            self.active_timer = threading.Timer(delay, self.task)
            self.active_timer.start()
        self.timer_lock.release()


def _create_leaderboard(game_data):
    all_mmrs = sorted([team['rating'] for team in game_data.teams()])
    clan_members = [
        team
        for team
        in game_data.teams()
        if "clan_link" in team["member"][0] and team["member"][0]["clan_link"]["clan_name"] == "All Inspiration"]
    sorted_clan_members = sorted(clan_members, key=lambda team: team['rating'], reverse=True)

    def extract_battle_tag(team):
        if "character_link" in team["member"][0]:
            return team["member"][0]["character_link"]["battle_tag"]
        else:
            return "UNKNOWN"

    def extract_race(team):
        return team["member"][0]["played_race_count"][0]["race"]["en_US"]

    def extract_percentile(team):
        return 1 - bisect.bisect(all_mmrs, team["rating"]) / len(all_mmrs)

    def pretty_percentile(percentile):
        return "{0:.2f}%".format(percentile * 100)

    return [
        {"battle_tag": extract_battle_tag(team),
         "race": extract_race(team),
         "mmr": team["rating"],
         "percentile": pretty_percentile(extract_percentile(team))}
        for team in sorted_clan_members]

_cache_lock = threading.Lock()
_leaderboard_cache = _create_leaderboard(sc2gamedata.download_ladder_data(_ACCESS_TOKEN))


def _refresh_cache():
    global _leaderboard_cache

    print("updating cache")
    leaderboard = _create_leaderboard(sc2gamedata.download_ladder_data(_ACCESS_TOKEN))

    _cache_lock.acquire()
    _leaderboard_cache = leaderboard
    _cache_lock.release()
    print("cache_updated")

_repeating_task_scheduler = RepeatingTaskScheduler(_refresh_cache)


@route('/')
def display_leaderboard():
    _repeating_task_scheduler.schedule(_REFRESH_INTERVAL)
    global _leaderboard_cache

    _cache_lock.acquire()
    leaderboard = copy.deepcopy(_leaderboard_cache)
    _cache_lock.release()

    return {"data": leaderboard}


if __name__ == "__main__":
    port = os.getenv('ALLIN_DATA_PORT', 9007)
    run(host='localhost', port=port, debug=True)
