from nba_api.stats.endpoints.teamgamelogs import TeamGameLogs as tgl
from nba_api.stats.static import teams
import pandas as pd
import numpy as np
import time
import math
from utility.reference import sql
import json
import requests

pd.options.mode.chained_assignment = None

START_SEASON = 1979
COLUMNS = [
    "TEAM_ID",
    "TEAM_ABBREVIATION",
    "TEAM_NAME",
    "GAME_ID",
    "GAME_DATE",
    "MATCHUP",
    "WL",
    "MIN",
    "FGM",
    "FGA",
    "FG_PCT",
    "FG3M",
    "FG3A",
    "FG3_PCT",
    "FTM",
    "FTA",
    "FT_PCT",
    "OREB",
    "DREB",
    "REB",
    "AST",
    "STL",
    "BLK",
    "TOV",
    "PF",
    "PTS",
    "PLUS_MINUS",
    "SEASON_YEAR",
    "TEAM",
    "OPPONENT",
    "HOME/AWAY",
]

NBA_TEAM_IDS = pd.DataFrame.from_records(teams.get_teams())["id"].to_list()


def scrape_game_logs(seasons):

    gamelogs_df = pd.DataFrame()

    for n in seasons:
        second_year = str(n + 1)[-2:]
        season_string_param = str(n) + "-" + second_year
        print(f"\nGetting gamelogs for {n} season...")

        for attempt in range(0, 5):
            try:
                season_json = tgl(
                    season_nullable=season_string_param
                ).get_dict()["resultSets"]
                headers = season_json[0]["headers"]
                data = season_json[0]["rowSet"]
                stage_df = pd.DataFrame(columns=headers, data=data)
                stage_df["SEASON_YEAR"] = n
                gamelogs_df = pd.concat([gamelogs_df, stage_df])
                time.sleep(0.6)
                break
            except json.decoder.JSONDecodeError:
                print("\n\t\tError, trying again...")
                time.sleep(0.5)
            except requests.exceptions.ReadTimeout:
                print("\n\t\tError, trying again...")
                time.sleep(3)
    gamelogs_df.dropna(inplace=True)

    gamelogs_df["TEAM"] = gamelogs_df["MATCHUP"].apply(lambda x: x.split()[0])
    gamelogs_df["OPPONENT"] = gamelogs_df["MATCHUP"].apply(lambda x: x.split()[-1])
    gamelogs_df["HOME/AWAY"] = gamelogs_df["MATCHUP"].apply(
        lambda x: "AWAY" if x.split()[1] == "@" else "HOME"
    )

    gamelogs_df.rename({"SEASON_YEAR": "SEASON"}, inplace=True)

    gamelogs_df.sort_values(
        by=["GAME_DATE", "TEAM_ABBREVIATION"], ascending=False, inplace=True
    )

    gamelogs_df = gamelogs_df.loc[gamelogs_df["TEAM_ID"].isin(NBA_TEAM_IDS)]

    gamelogs_df.reset_index(drop=True, inplace=True)

    return gamelogs_df[COLUMNS]


def get_latest_season():
    query = """
    SELECT MAX("SEASON_YEAR") as "SEASON" FROM nba_gamelogs.team_gamelogs    
    """
    return sql.convert_sql_to_df(query=query)["SEASON"].max()


def lambda_handler(event, context):
    main()


def drop_current_season_rows(season_year):
    statement = (
        f'DELETE FROM nba_gamelogs.team_gamelogs WHERE "SEASON_YEAR" = {season_year}'
    )
    sql.execute_database_operations(statement)


def main():
    try:
        start_season = get_latest_season()
        drop_current_season_rows(start_season)
    except Exception:
        start_season = START_SEASON
    seasons = [n for n in range(start_season, 2026)]
    number_of_batches = math.ceil(len(seasons) / 5)

    for x in range(0, number_of_batches):
        season_batch = seasons[(x * 5) :]
        if len(season_batch) >= 5:
            season_batch = season_batch[:5]
        print(f"\nGrabbing gamelogs for {season_batch} seasons...\n")

        gamelog_df = scrape_game_logs(season_batch)

        behavior = "replace" if x == 0 and start_season == START_SEASON else "append"
        sql.export_df_to_sql(
            gamelog_df,
            table_name="team_gamelogs",
            schema="nba_gamelogs",
            behavior=behavior,
        )


if __name__ == "__main__":
    main()
