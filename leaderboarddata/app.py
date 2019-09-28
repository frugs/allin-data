import functools
import itertools
import json
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

_TIME_THRESHOLD = 10
_LEAGUE_IDS = range(7)
_THREADS = min(5, len(_LEAGUE_IDS))

_CLAN_IDS = [369458, 40715, 406747]

_GUESTS = [
    "Tbbdd#6920",
    "MrLando#1626",
    "eXiled#1678",
    "IMeXiled#1893",
    "Andy#12473",
    "Sympathy#1701",
]

_PAGE_SIZE = 500
_CACHE_EXPIRY = 10 * 60
_LOCK_TIMEOUT = 60

firebase_admin.initialize_app(options=_FIREBASE_CONFIG)
app = flask.Flask(__name__)


def _flatten(l) -> list:
    return list(itertools.chain.from_iterable(l))


def _fetch_paginated(
    ref: firebase_admin.db.Reference, start_after_member_key: str
) -> dict:
    query = ref.order_by_key().limit_to_first(_PAGE_SIZE)
    if start_after_member_key:
        query = query.start_at(start_after_member_key)
    members = query.get()
    members = members if members else {}
    if start_after_member_key:
        members.pop(start_after_member_key, None)
    return members


def _fetch_registered_members(
    db: firebase_admin.db.Reference, start_after_member_key: str = None
) -> dict:
    return _fetch_paginated(db.child("members"), start_after_member_key)


def _fetch_unregistered_members(
    db: firebase_admin.db.Reference, start_after_member_key: str = None
) -> dict:
    return _fetch_paginated(
        db.child("unregistered_members").child("us"), start_after_member_key
    )


def _find_highest_ranked_character(current_season_id: int, characters: dict) -> dict:
    highest_ranked_character = ("", 0)
    for character_key, character in characters.items():
        ladder_infos = (
            character.get("ladder_info", {}).get(str(current_season_id), {}).values()
        )
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
        member_data.get("battle_tag"),
        character_name,
        "UNKNOWN",
    ]

    return next(iter(name for name in names if name))


def _format_percentile(percentile: float) -> str:
    return "{0:.2f}%".format(percentile)


def _extract_registered_member_leaderboard_infos(
    current_season_id: int, member: dict
) -> list:
    if not member.get("is_full_member", False):
        return []

    characters = member.get("characters", {}).get("us", {})

    if not characters:
        return []

    highest_ranked_character = _find_highest_ranked_character(
        current_season_id, characters
    )

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
            "percentile": _format_percentile(ladder_info.get("percentile", 100.0)),
            "race": race,
        }
        for race, ladder_info in ladder_infos.items()
    ]


def _extract_unregistered_member_leaderboard_infos(
    current_season_id: int, character_key: str, member: dict
) -> list:
    battle_tag = member.get("battle_tag", "")
    character_name = character_key.split("-")[-1]

    ladder_infos = member.get("ladder_info", {}).get(str(current_season_id), {})

    display_name = "{} ({})".format(battle_tag, character_name)

    return [
        {
            "type": "player",
            "name": display_name,
            "league": ladder_info.get("league_id", 0),
            "mmr": ladder_info.get("mmr", 0),
            "percentile": _format_percentile(ladder_info.get("percentile", 100.0)),
            "race": race,
        }
        for race, ladder_info in ladder_infos.items()
    ]


def _fetch_tier_boundaries(season_id: int) -> list:
    tier_boundaries_db = (
        firebase_admin.db.reference()
        .child("tier_boundaries")
        .child("us")
        .child(str(season_id))
        .get()
    )
    tier_boundaries = list(tier_boundaries_db) if tier_boundaries_db else []
    tier_boundaries.sort(key=lambda x: x["tier"], reverse=True)
    return tier_boundaries


def _create_leaderboard():
    access_token, _ = sc2gamedata.get_access_token(_CLIENT_ID, _CLIENT_SECRET, "us")
    season_id = sc2gamedata.get_current_season_data(access_token)["id"]
    db = firebase_admin.db.reference()

    registered_members = _fetch_registered_members(db)
    registered_member_leaderboard_infos = []
    while registered_members:
        registered_member_leaderboard_infos += map(
            functools.partial(_extract_registered_member_leaderboard_infos, season_id),
            registered_members.values(),
        )
        if len(registered_members) < _PAGE_SIZE:
            registered_members = {}
        else:
            registered_members = _fetch_registered_members(
                db, next(reversed(registered_members))
            )

    flattened_registered_member_leaderboard_infos = _flatten(
        registered_member_leaderboard_infos
    )

    unregistered_members = _fetch_unregistered_members(db)
    unregistered_member_leaderboard_infos = []
    while unregistered_members:
        unregistered_member_leaderboard_infos += map(
            functools.partial(
                _extract_unregistered_member_leaderboard_infos, season_id
            ),
            unregistered_members.keys(),
            unregistered_members.values(),
        )
        if len(unregistered_members) < _PAGE_SIZE:
            unregistered_members = {}
        else:
            unregistered_members = _fetch_unregistered_members(
                db, next(reversed(unregistered_members))
            )

    flattened_unregistered_member_leaderboard_infos = _flatten(
        unregistered_member_leaderboard_infos
    )

    leaderboard_infos = (
        flattened_registered_member_leaderboard_infos
        + flattened_unregistered_member_leaderboard_infos
    )
    leaderboard_infos.sort(key=lambda x: x["mmr"], reverse=True)

    tier_boundaries = _fetch_tier_boundaries(season_id)

    # Handle grandmaster league
    result = [tier_boundaries.pop(0)]

    for leaderboard_info in leaderboard_infos:
        mmr = leaderboard_info["mmr"]
        while tier_boundaries and mmr < tier_boundaries[0]["max_mmr"]:
            result.append(tier_boundaries.pop(0))
        result.append(leaderboard_info)

    return result


_leaderboard_cache = []
_last_update_time = 0
_cache_lock = threading.RLock()


@app.route("/")
def display_leaderboard():
    global _leaderboard_cache
    global _last_update_time
    global _cache_lock

    request_time = time.time()
    if request_time > _last_update_time + _CACHE_EXPIRY:
        _cache_lock.acquire(timeout=_LOCK_TIMEOUT)

        # An update may have happened while we were waiting for the lock, so check again
        if request_time > _last_update_time + _CACHE_EXPIRY:
            _leaderboard_cache = _create_leaderboard()
            _last_update_time = time.time()

        _cache_lock.release()

    return flask.jsonify({"data": _leaderboard_cache})
