import bisect
import copy
import os
import threading
import typing
import multiprocessing
import itertools
import sc2gamedata
from bottle import route, run

_ACCESS_TOKEN = os.getenv('BATTLE_NET_ACCESS_TOKEN', "")
_REFRESH_INTERVAL = 30
_LEAGUE_COUNT = 6


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


def _for_each_league(current_season_id: int, league_id: int):
    league_mmrs = []
    clan_teams = []
    tiers = []

    league_data = sc2gamedata.get_league_data(_ACCESS_TOKEN, current_season_id, league_id)
    for tier_index, tier_data in enumerate(reversed(league_data["tier"])):
        tier_id = (league_id * 3) + tier_index
        tiers.append({"tier_id": tier_id, "tier_data": tier_data})
        for division_data in tier_data["division"]:
            ladder_data = sc2gamedata.get_ladder_data(_ACCESS_TOKEN, division_data["ladder_id"])
            if "team" in ladder_data:
                for team_data in ladder_data["team"]:
                    league_mmrs.append(team_data["rating"])

                    if "clan_link" in team_data["member"][0] and team_data["member"][0]["clan_link"]["clan_name"] == "All Inspiration":
                        team_data["tier_id"] = tier_id
                        clan_teams.append(team_data)

    return clan_teams, league_mmrs, tiers


def _create_leaderboard():
    season_id = sc2gamedata.get_current_season_data(_ACCESS_TOKEN)["id"]
    league_ids = range(_LEAGUE_COUNT)

    with multiprocessing.Pool(_LEAGUE_COUNT) as p:
        result = p.starmap(_for_each_league, zip([season_id] * _LEAGUE_COUNT, league_ids))

    clan_members_by_league, all_mmrs_by_league, tiers_by_league = map(list, zip(*result))
    all_mmrs = sorted(list(
        itertools.chain.from_iterable(all_mmrs_by_league)))
    clan_members = sorted(list(
        itertools.chain.from_iterable(clan_members_by_league)), key=lambda team: team['rating'], reverse=True)
    tiers = sorted(list(
        itertools.chain.from_iterable(tiers_by_league)), key=lambda tier: tier['tier_id'], reverse=True)

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

    result = []
    for clan_member in clan_members:
        while tiers and clan_member["rating"] < tiers[0]["tier_data"]["max_rating"]:
            tier = tiers.pop(0)
            result.append({"type": "boundary",
                           "tier": tier["tier_id"],
                           "min_mmr": tier["tier_data"]["min_rating"],
                           "max_mmr": tier["tier_data"]["max_rating"]})
        result.append({
            "type": "player",
            "battle_tag": extract_battle_tag(clan_member),
            "race": extract_race(clan_member),
            "tier": clan_member["tier_id"],
            "mmr": clan_member["rating"],
            "percentile": pretty_percentile(extract_percentile(clan_member))})

    return result

_cache_lock = threading.Lock()
_leaderboard_cache = None


def _refresh_cache():
    global _leaderboard_cache

    print("updating cache")
    leaderboard = _create_leaderboard()

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
    _refresh_cache()
    port = os.getenv('ALLIN_DATA_PORT', 9007)
    run(host='localhost', port=port, debug=True)
