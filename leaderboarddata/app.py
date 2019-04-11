import bisect
import functools
import itertools
import json
import multiprocessing.pool
import threading
import time

import flask
import sc2gamedata
import firebase_admin
import firebase_admin.db

from google.cloud import datastore


def _retrieve_config_value(key: str) -> str:
    datastore_client = datastore.Client()
    return datastore_client.get(datastore_client.key("Config", key))["value"]


_CLIENT_ID = _retrieve_config_value("blizzardClientKey")
_CLIENT_SECRET = _retrieve_config_value("blizzardClientSecret")
_FIREBASE_CONFIG = json.loads(_retrieve_config_value("firebaseConfig"))

_TIME_THRESHOLD = 60
_LEAGUE_IDS = range(7)
_THREADS = min(5, len(_LEAGUE_IDS))

_CLAN_IDS = [369458, 40715, 406747]

_GUESTS = [
    "Tbbdd#6920", "MrLando#1626", "eXiled#1678", "IMeXiled#1893", "Andy#12473", "Sympathy#1701"
]

firebase_admin.initialize_app(options=_FIREBASE_CONFIG)


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


def _flatten(l) -> list:
    return list(itertools.chain.from_iterable(l))


def _fetch_members() -> list:
    ref = firebase_admin.db.reference()
    members = ref.child("members").get(shallow=True)
    return list(members.keys())


def _fetch_member(member_id: str) -> dict:
    ref = firebase_admin.db.reference()
    member = ref.child("members").child(member_id).get()
    return member if member else {}


def _find_highest_ranked_character(current_season_id: int, characters: dict) -> dict:
    highest_ranked_character = ("", 0)
    for character_key, character in characters.items():
        ladder_infos = character.get("ladder_info", {}).get(str(current_season_id), {}).values()
        for ladder_info in ladder_infos:
            mmr = ladder_info.get("mmr", 0)
            if mmr > highest_ranked_character[1]:
                highest_ranked_character = (character_key, mmr)

    return characters.get(highest_ranked_character[0], {})


def _create_display_name(member_data: dict, character_name: str) -> str:
    names = [
        member_data.get("discord_display_name"),
        member_data.get("discord_server_nick"),
        member_data.get("discord_username"),
        member_data.get("battle_tag"), character_name, "UNKNOWN"
    ]

    return next(iter(name for name in names if name))


def _calculate_percentile_for_mmr(mmr: int, mmrs: list) -> str:
    percentile = 1 - bisect.bisect(mmrs, mmr) / len(mmrs)
    return "{0:.2f}%".format(percentile * 100)


def _fetch_leaderboard_info_for_member(current_season_id: int, mmrs: list, member_id: str) -> list:
    member = _fetch_member(member_id)
    if not member.get("is_full_member", False):
        return []

    characters = member.get("characters", {}).get("us", {})

    if not characters:
        return []

    highest_ranked_character = _find_highest_ranked_character(current_season_id, characters)

    if not highest_ranked_character:
        return []

    character_name = highest_ranked_character.get("name")

    ladder_infos = highest_ranked_character["ladder_info"][str(current_season_id)]

    display_name = _create_display_name(member, character_name)

    return [
        {
            "type": "player",
            "name": display_name,
            "league": ladder_info.get("league_id", 0),
            "mmr": ladder_info.get("mmr", 0),
            "percentile": _calculate_percentile_for_mmr(ladder_info.get("mmr", 0), mmrs),
            "race": race,
        } for race, ladder_info in ladder_infos.items()
    ]


def _fetch_tier_boundaries_for_league(
    access_token: str, current_season_id: int, league_id: int
) -> list:
    league_data = sc2gamedata.get_league_data(access_token, current_season_id, league_id)
    return [
        {
            "type": "boundary",
            "tier": (league_id * 3) + tier_index,
            "min_mmr": tier_data.get("min_rating", 0),
            "max_mmr": tier_data.get("max_mmr", 99999),
        } for tier_index, tier_data in enumerate(reversed(league_data.get("tier", [])))
    ]


def _fetch_mmrs_for_division(access_token: str, ladder_id: int) -> list:
    ladder_data = sc2gamedata.get_ladder_data(access_token, ladder_id)
    return [team.get("rating") for team in ladder_data.get("team", []) if team.get("rating")]


def _fetch_mmrs_for_each_league(access_token: str, current_season_id: int, league_id: int) -> list:
    league_data = sc2gamedata.get_league_data(access_token, current_season_id, league_id)
    tiers = league_data.get("tier", [])
    divisions = [tier.get("division", []) for tier in tiers]
    flattened_divisions = _flatten(divisions)
    return _flatten(
        [
            _fetch_mmrs_for_division(access_token, division["ladder_id"])
            for division in flattened_divisions if division.get("ladder_id")
        ]
    )


def _create_leaderboard():
    access_token, _ = sc2gamedata.get_access_token(_CLIENT_ID, _CLIENT_SECRET, "us")
    season_id = sc2gamedata.get_current_season_data(access_token)["id"]

    members = _fetch_members()

    with multiprocessing.pool.ThreadPool(_THREADS) as p:
        mmrs = p.map(
            functools.partial(_fetch_mmrs_for_each_league, access_token, season_id), _LEAGUE_IDS
        )
        flattened_mmrs = _flatten(mmrs)
        flattened_mmrs.sort()

        leaderboard_infos = p.map(
            functools.partial(_fetch_leaderboard_info_for_member, season_id, flattened_mmrs),
            members
        )
        flattened_leaderboard_infos = _flatten(leaderboard_infos)
        flattened_leaderboard_infos.sort(key=lambda x: x["mmr"], reverse=True)

        tier_boundaries = p.map(
            functools.partial(_fetch_tier_boundaries_for_league, access_token, season_id),
            _LEAGUE_IDS
        )
        flattened_tier_boundaries = _flatten(tier_boundaries)
        flattened_tier_boundaries.sort(key=lambda x: x["max_mmr"], reverse=True)

    # Handle grandmaster league
    result = [flattened_tier_boundaries.pop(0)]

    for leaderboard_info in flattened_leaderboard_infos:
        mmr = leaderboard_info["mmr"]
        while flattened_tier_boundaries and mmr < flattened_tier_boundaries[0]["max_mmr"]:
            result.append(flattened_tier_boundaries.pop(0))
        result.append(leaderboard_info)

    return result


_leaderboard_cache = []
_is_currently_updating = AtomicBoolean(False)
_last_request_time = 0


@app.route("/update")
def update_leaderboard():
    global _leaderboard_cache
    global _last_request_time

    if (
        not _is_currently_updating.get_and_set(True)
        and (not _leaderboard_cache or _last_request_time - time.time() < _TIME_THRESHOLD)
    ):
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
