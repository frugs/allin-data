import bisect
import os
import threading
import typing
import multiprocessing
import itertools
import pickle
import asyncio
import functools
import retryfallback
import sc2gamedata
import pyrebase
import growler

_CLIENT_ID = os.getenv('BATTLE_NET_CLIENT_ID', "")
_CLIENT_SECRET = os.getenv('BATTLE_NET_CLIENT_SECRET', "")
_REFRESH_INTERVAL = 30
_LEAGUE_COUNT = 7

_CLAN_IDS = [
    369458,
    40715
]

_GUESTS = [
    "Tbbdd#6920",
    "taylorqt#1543",
    "MrLando#1626",
    "eXiled#1678",
    "IMeXiled#1893",
    "Matlo#1298",
    "Luneth#11496",
    "Andy#12473"
]


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


def _for_each_league(access_token: str, current_season_id: int, league_id: int):
    league_mmrs = []
    clan_teams = []
    tiers = []

    league_data = sc2gamedata.get_league_data(access_token, current_season_id, league_id)
    for tier_index, tier_data in enumerate(reversed(league_data["tier"])):
        tier_id = (league_id * 3) + tier_index
        tiers.append({"tier_id": tier_id, "tier_data": tier_data})
        for division_data in tier_data["division"]:
            ladder_data = sc2gamedata.get_ladder_data(access_token, division_data["ladder_id"])
            if "team" in ladder_data:
                for team_data in ladder_data["team"]:
                    league_mmrs.append(team_data["rating"])
                    member_data = team_data["member"][0]
                    if ("clan_link" in member_data and member_data["clan_link"]["id"] in _CLAN_IDS)\
                            or "character_link" in member_data and member_data["character_link"]["battle_tag"] in _GUESTS:
                        team_data["tier_id"] = tier_id
                        clan_teams.append(team_data)

    return clan_teams, league_mmrs, tiers


def open_db() -> pyrebase.pyrebase.Database:
    with open("firebase.cfg", "rb") as file:
        db_config = pickle.load(file)

    return pyrebase.initialize_app(db_config).database()


def _create_leaderboard():
    access_token, _ = sc2gamedata.get_access_token(_CLIENT_ID, _CLIENT_SECRET, "us")
    season_id = sc2gamedata.get_current_season_data(access_token)["id"]
    league_ids = range(_LEAGUE_COUNT)

    with multiprocessing.Pool(_LEAGUE_COUNT) as p:
        result = p.starmap(functools.partial(_for_each_league, access_token), zip([season_id] * _LEAGUE_COUNT, league_ids))

    clan_members_by_league, all_mmrs_by_league, tiers_by_league = map(list, zip(*result))
    all_mmrs = sorted(list(
        itertools.chain.from_iterable(all_mmrs_by_league)))
    clan_members = sorted(list(
        itertools.chain.from_iterable(clan_members_by_league)), key=lambda team: team['rating'], reverse=True)
    tiers = sorted(list(
        itertools.chain.from_iterable(tiers_by_league)), key=lambda tier: tier['tier_id'], reverse=True)

    def extract_name(team):
        if "character_link" in team["member"][0]:
            battle_tag = team["member"][0]["character_link"]["battle_tag"]
        else:
            return "UNKNOWN"

        db = retryfallback.retry_callable(open_db, 10, None)
        if not db:
            return battle_tag

        query = db.child("members").order_by_child("caseless_battle_tag").equal_to(battle_tag.casefold())
        query_result = retryfallback.retry_callable(query.get, 10, pyrebase.pyrebase.PyreResponse([], ""))
        if not query_result.pyres:
            return battle_tag

        result_data = next(iter(query_result.val().values()))
        return result_data.get("discord_server_nick", result_data.get("discord_display_name", battle_tag))

    def extract_race(team):
        return team["member"][0]["played_race_count"][0]["race"]["en_US"]

    def extract_percentile(team):
        return 1 - bisect.bisect(all_mmrs, team["rating"]) / len(all_mmrs)

    def pretty_percentile(percentile):
        return "{0:.2f}%".format(percentile * 100)

    result = []

    # Handle grandmaster league
    grandmaster = tiers.pop(0)
    result.append({"type": "boundary",
                   "tier": grandmaster["tier_id"]})

    for clan_member in clan_members:
        while tiers and clan_member["rating"] < tiers[0]["tier_data"]["max_rating"]:
            tier = tiers.pop(0)
            result.append({"type": "boundary",
                           "tier": tier["tier_id"],
                           "min_mmr": tier["tier_data"]["min_rating"],
                           "max_mmr": tier["tier_data"]["max_rating"]})
        data = {
            "type": "player",
            "name": extract_name(clan_member),
            "race": extract_race(clan_member),
            "tier": clan_member["tier_id"],
            "mmr": clan_member["rating"],
            "percentile": pretty_percentile(extract_percentile(clan_member))
        }
        result.append(data)

    return result

print("initialising leaderboard")
_leaderboard_cache = _create_leaderboard()
_is_cache_update_scheduled = False


async def _refresh_cache():
    global _leaderboard_cache
    global _is_cache_update_scheduled

    await asyncio.sleep(_REFRESH_INTERVAL)

    print("updating cache")
    try:
        _leaderboard_cache = await asyncio.get_event_loop().run_in_executor(None, _create_leaderboard)
        print("cache_updated")
    except Exception as e:
        print(str(e))
        print("cache update failed")
        pass

    _is_cache_update_scheduled = False


web_app = growler.App('allinbot_controller')


@web_app.get('/')
def display_leaderboard(req, res):
    global _leaderboard_cache
    global _is_cache_update_scheduled

    if not _is_cache_update_scheduled:
        _is_cache_update_scheduled = True

        print("cache update scheduled")
        asyncio.ensure_future(_refresh_cache())

    res.send_json({"data": _leaderboard_cache})

if __name__ == "__main__":
    print("starting web server")
    port = os.getenv('ALLIN_DATA_PORT', 9007)
    asyncio.Server = web_app.create_server(port=port)
    asyncio.get_event_loop().run_forever()
