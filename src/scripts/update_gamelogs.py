from nba_api.stats.endpoints import CommonPlayerInfo
from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog
from datetime import date
from utility.reference import sql
from utility.logger import get_struct_logger
import datetime
import pandas as pd
import time
import json
import requests
import os

log = get_struct_logger()

HEADERS = sql.convert_sql_to_df(
    query="SELECT column_name \
                                FROM information_schema.columns \
                                WHERE table_name = 'player_gamelogs';"
)["column_name"].to_list()
TODAY = date.today()
ACTIVE_PLAYERS_DF = pd.DataFrame.from_records(players.get_active_players())
MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

def find_latest_game_date(current_gamelog_df):
    log.info("Finding last gamedate...")

    query = """
    SELECT MAX("GAME_DATE") as "GAME_DATE" 
    FROM nba_gamelogs.player_gamelogs;
    """
    game_date = sql.convert_sql_to_df(query=query)

    last_date = pd.to_datetime(
        game_date["GAME_DATE"].max()
    ).date() + datetime.timedelta(days=1)
    last_date = str(last_date)
    convert = last_date.split("-")
    year = convert[0]
    month = convert[1]
    day = convert[2]

    return month + "/" + day + "/" + year


def get_new_logs(last_game_date):
    log.info("Grabbing new gamelogs...")

    new_game_logs = []

    player_dict = players.get_active_players()

    for x in player_dict:
        for attempt in range(0, 5):
            try:
                log.info("Grabbing gamelogs", player_name=x["full_name"])
                new_game_logs.append(
                    playergamelog.PlayerGameLog(
                        player_id=x["id"], date_from_nullable=last_game_date
                    )
                )
                time.sleep(0.500)
                break
            except json.decoder.JSONDecodeError as e:
                log.error("Error, trying again...", e=e, attempt=attempt)
                time.sleep(0.5)
            except requests.exceptions.ReadTimeout as e:
                log.error("Error, trying again...", e=e, attempt=attempt)
                time.sleep(3)

    return new_game_logs


def convert_new_logs_to_df(new_logs):
    headers = new_logs[0].get_dict()["resultSets"][0]["headers"]
    new_logs_df = pd.DataFrame(columns=headers)
    for x in new_logs:
        for n in x.get_dict()["resultSets"][0]["rowSet"]:
            new_logs_df.loc[len(new_logs_df)] = n

    new_logs_df["TEAM"] = new_logs_df["MATCHUP"].apply(get_team)
    new_logs_df["OPPONENT"] = new_logs_df["MATCHUP"].apply(get_opponent)
    new_logs_df["HOME/AWAY"] = new_logs_df["MATCHUP"].apply(get_home_away)

    standard_date = lambda x: (
        str(
            x.split(" ")[2]
            + "-"
            + str(MONTHS[x.split(" ")[0]])
            + "-"
            + x.split(" ")[1].rstrip(",")
        )
        if x.split(" ")[0] in MONTHS
        else x
    )

    new_logs_df["GAME_DATE"] = new_logs_df.GAME_DATE.apply(standard_date)
    new_logs_df["GAME_DATE"] = pd.to_datetime(new_logs_df["GAME_DATE"])

    new_logs_df = new_logs_df.merge(
        ACTIVE_PLAYERS_DF, how="inner", left_on="Player_ID", right_on="id"
    ).drop(columns=["id", "first_name", "last_name"])

    new_logs_df.rename({"full_name": "player_name"}, axis=1, inplace=True)

    new_logs_df.sort_values(by=["GAME_DATE", "TEAM"], ascending=False, inplace=True)

    new_logs_df = new_logs_df.drop_duplicates().reset_index(drop=True)

    return new_logs_df


def clean_matchup_column(matchup):
    matchup = matchup.split()
    team = matchup[0]
    opponent = matchup[-1]
    if matchup[1] == "@":
        home_away = "AWAY"
    else:
        home_away = "HOME"

    return team, opponent, home_away


def get_team(matchup):
    team, opponent, home_away = clean_matchup_column(matchup)
    return team


def get_opponent(matchup):
    team, opponent, home_away = clean_matchup_column(matchup)
    return opponent


def get_home_away(matchup):
    team, opponent, home_away = clean_matchup_column(matchup)
    return home_away


def lambda_handler(event, context):
    main()


def main():
    latest_game_date = find_latest_game_date()
    new_logs = get_new_logs(latest_game_date)
    new_logs_df = convert_new_logs_to_df(new_logs)

    new_logs_df = new_logs_df[HEADERS]

    updated_logs = pd.concat([current_gamelog_df, new_logs_df])

    updated_logs.drop_duplicates(inplace=True)

    del current_gamelog_df

    log.info("Exporting to sql db...")
    sql.export_df_to_sql(
        df=updated_logs,
        table_name="player_gamelogs",
        schema="nba_gamelogs",
        behavior="replace",
    )
    log.info("Export successful.")


if __name__ == "__main__":
    main()
