import bisect
import os
import json
from bottle import route, run
import sc2gamedata

_ACCESS_TOKEN = os.getenv('BATTLE_NET_ACCESS_TOKEN', "")


def get_leaderboard() -> list:
    game_data = sc2gamedata.download_ladder_data(_ACCESS_TOKEN)

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


@route('/')
def hello():
    return {"data": get_leaderboard()}


if __name__ == "__main__":
    port = os.getenv('ALLIN_DATA_PORT', 9007)
    run(host='localhost', port=port, debug=True)
