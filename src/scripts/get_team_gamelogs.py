from nba_api.stats.endpoints.teamgamelogs import TeamGameLogs as tgl
import pandas as pd
import numpy as np
import time
import math
from utility.reference import sql
import json
import requests

pd.options.mode.chained_assignment = None

START_SEASON = 2013  # most historical data can be aggregated from player logs. Team logs is mainly to simplify model prediction pipeline


def scrape_game_logs(seasons):

    gamelogs_df = pd.DataFrame()

    for n in seasons:
        second_year = str(n + 1)[-2:]
        season_string_param = str(n) + "-" + second_year
        print(f"\nGetting gamelogs for {n} season...")

        for attempt in range(0, 5):
            try:
                season_json = tgl(season_nullable=season_string_param).get_dict()[
                    "resultSets"
                ]
                headers = season_json[0]["headers"]
                data = season_json[0]["rowSet"]
                stage_df = pd.DataFrame(columns=headers, data=data)
                stage_df['SEASON_YEAR'] = n
                gamelogs_df = pd.concat(
                    [gamelogs_df, stage_df]
                )
                time.sleep(0.6)
                break
            except json.decoder.JSONDecodeError:
                print("\n\t\tError, trying again...")
                time.sleep(0.5)
            except requests.exceptions.ReadTimeout:
                print("\n\t\tError, trying again...")
                time.sleep(3)

    gamelogs_df["TEAM"] = gamelogs_df["MATCHUP"].apply(lambda x: x.split()[0])
    gamelogs_df["OPPONENT"] = gamelogs_df["MATCHUP"].apply(lambda x: x.split()[-1])
    gamelogs_df["HOME/AWAY"] = gamelogs_df["MATCHUP"].apply(
        lambda x: "AWAY" if x.split()[1] == "@" else "HOME"
    )

    gamelogs_df.rename({"SEASON_YEAR": "SEASON"}, inplace=True)

    gamelogs_df.sort_values(
        by=["GAME_DATE", "TEAM_ABBREVIATION"], ascending=False, inplace=True
    )

    gamelogs_df.reset_index(drop=True, inplace=True)

    return gamelogs_df


def main():
    seasons = [n for n in range(START_SEASON, 2025)]
    number_of_batches = math.ceil(len(seasons) / 5)

    for x in range(0, number_of_batches):
        season_batch = seasons[(x * 5) :]
        if len(season_batch) >= 5:
            season_batch = season_batch[:5]
        print(f"\nGrabbing gamelogs for {season_batch} seasons...\n")

        gamelog_df = scrape_game_logs(season_batch)
        sql.export_df_to_sql(
            gamelog_df,
            table_name="team_gamelogs",
            schema="nba_gamelogs",
            behavior="append",
        )


if __name__ == "__main__":
    main()
