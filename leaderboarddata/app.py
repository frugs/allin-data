import bisect
import functools
import itertools
import json
import multiprocessing.pool
import threading
import time

import flask
import pyrebase
import retryfallback
import sc2gamedata

from google.cloud import datastore


def retrieve_config_value(key: str) -> str:
    datastore_client = datastore.Client()
    return datastore_client.get(datastore_client.key("Config", key))["value"]


_CLIENT_ID = retrieve_config_value("blizzardClientKey")
_CLIENT_SECRET = retrieve_config_value("blizzardClientSecret")
_FIREBASE_CONFIG = json.loads(retrieve_config_value("firebaseConfig"))

_TIME_THRESHOLD = 60
_LEAGUE_IDS = range(7)
_THREADS = min(5, len(_LEAGUE_IDS))

_CLAN_IDS = [369458, 40715, 406747]

_GUESTS = [
    "Tbbdd#6920", "MrLando#1626", "eXiled#1678", "IMeXiled#1893", "Andy#12473", "Sympathy#1701"
]


class AtomicBoolean:
    def __init__(self, initial_value: bool):
        self.value = initial_value
        self.lock = threading.Lock()

    def get_and_set(self, value: bool) -> bool:
        self.lock.acquire()
        old_value = self.value
        self.value = value
        self.lock.release()
        return old_value


app = flask.Flask(__name__)


def _for_each_league(access_token: str, current_season_id: int, league_id: int):
    league_mmrs = []
    clan_teams = []
    tiers = []

    league_data = sc2gamedata.get_league_data(access_token, current_season_id, league_id)
    for tier_index, tier_data in enumerate(reversed(league_data["tier"])):
        tier_id = (league_id * 3) + tier_index
        tiers.append({
            "tier_id": tier_id,
            "tier_data": {
                "min_rating": tier_data.get("min_rating", 0),
                "max_rating": tier_data.get("max_rating", 0)
            }
        })
        for division_data in tier_data.get("division", []):
            ladder_data = sc2gamedata.get_ladder_data(access_token, division_data["ladder_id"])
            if "team" in ladder_data:
                for team_data in ladder_data["team"]:
                    league_mmrs.append(team_data["rating"])

                    if "member" in team_data:
                        member_data = team_data["member"][0]
                        if ("clan_link" in member_data and member_data["clan_link"]["id"] in _CLAN_IDS)\
                                or "character_link" in member_data and member_data["character_link"]["battle_tag"] in _GUESTS:
                            team_data["tier_id"] = tier_id
                            clan_teams.append(team_data)

    return clan_teams, league_mmrs, tiers


def open_db() -> pyrebase.pyrebase.Database:
    return pyrebase.initialize_app(_FIREBASE_CONFIG).database()


def _create_leaderboard():
    access_token, _ = sc2gamedata.get_access_token(_CLIENT_ID, _CLIENT_SECRET, "us")
    season_id = sc2gamedata.get_current_season_data(access_token)["id"]

    with multiprocessing.pool.ThreadPool(_THREADS) as p:
        result = p.map(functools.partial(_for_each_league, access_token, season_id), _LEAGUE_IDS)

    clan_members_by_league, all_mmrs_by_league, tiers_by_league = map(list, zip(*result))
    all_mmrs = sorted(list(itertools.chain.from_iterable(all_mmrs_by_league)))
    clan_members = sorted(
        list(itertools.chain.from_iterable(clan_members_by_league)),
        key=lambda team: team['rating'],
        reverse=True)
    tiers = sorted(
        list(itertools.chain.from_iterable(tiers_by_league)),
        key=lambda tier: tier['tier_id'],
        reverse=True)

    def extract_name(team):
        if "member" in team and "character_link" in team["member"][0]:
            battle_tag = team["member"][0]["character_link"]["battle_tag"]
        else:
            return "UNKNOWN"

        db = retryfallback.retry_callable(open_db, 10, None)
        if not db:
            return battle_tag

        query = db.child("members").order_by_child("caseless_battle_tag").equal_to(
            battle_tag.casefold())
        query_result = retryfallback.retry_callable(query.get, 10,
                                                    pyrebase.pyrebase.PyreResponse([], ""))
        if not query_result.pyres:
            return battle_tag

        result_data = next(iter(query_result.val().values()))
        return result_data.get("discord_display_name",
                               result_data.get("discord_server_nick", battle_tag))

    def extract_race(team):
        if "member" in team:
            return team["member"][0]["played_race_count"][0]["race"]["en_US"]
        else:
            return "UNKNOWN"

    def extract_percentile(team):
        return 1 - bisect.bisect(all_mmrs, team["rating"]) / len(all_mmrs)

    def pretty_percentile(percentile):
        return "{0:.2f}%".format(percentile * 100)

    result = []

    # Handle grandmaster league
    grandmaster = tiers.pop(0)
    result.append({"type": "boundary", "tier": grandmaster["tier_id"]})

    for clan_member in clan_members:
        while tiers and clan_member["rating"] < tiers[0]["tier_data"]["max_rating"]:
            tier = tiers.pop(0)
            result.append({
                "type": "boundary",
                "tier": tier["tier_id"],
                "min_mmr": tier["tier_data"]["min_rating"],
                "max_mmr": tier["tier_data"]["max_rating"]
            })
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


_leaderboard_cache = []
_is_currently_updating = AtomicBoolean(False)
_last_request_time = time.gmtime(0)


@app.route("/update")
def update_leaderboard():
    global _leaderboard_cache
    global _last_request_time

    if (not _is_currently_updating.get_and_set(True)
            and (not _leaderboard_cache or _last_request_time - time.time() < _TIME_THRESHOLD)):
        try:
            _leaderboard_cache = _create_leaderboard()
            print("cache_updated")
        finally:
            _is_currently_updating.get_and_set(False)

    return ""


@app.route("/")
def display_leaderboard():
    global _leaderboard_cache
    global _last_request_time

    _last_request_time = time.time()
    return flask.jsonify({"data": _leaderboard_cache})
