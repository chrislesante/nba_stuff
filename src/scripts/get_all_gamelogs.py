from nba_api.stats.endpoints import playergamelog
import pandas as pd
import numpy as np
import time
import math
from utility.reference import sql
import json
import requests

pd.options.mode.chained_assignment = None

PLAYER_DF = sql.convert_sql_to_df("all_historical_players", "general")
HEADERS = playergamelog.PlayerGameLog(
    player_id=PLAYER_DF.loc[0, "PERSON_ID"]
).get_dict()["resultSets"][0]["headers"]


def scrape_game_logs(player_df, seasons):

    game_logs = []

    for n in seasons:
        print(f"\nGetting gamelogs for {n} season...")
        players = player_df[
            (player_df["FROM_YEAR"] <= n) & (player_df["TO_YEAR"] >= n)
        ][["PERSON_ID", "DISPLAY_FIRST_LAST"]].copy()
        for i, row in players.iterrows():
            for attempt in range(0, 5):
                try:
                    print(
                        f"\n\tGrabbing gamelogs for player {row['DISPLAY_FIRST_LAST']}"
                    )
                    game_logs.extend(
                        playergamelog.PlayerGameLog(
                            player_id=row["PERSON_ID"], season=n
                        ).get_dict()["resultSets"][0]["rowSet"]
                    )
                    time.sleep(1)
                    break
                except json.decoder.JSONDecodeError:
                    print("\n\t\tError, trying again...")
                    time.sleep(0.5)
                except requests.exceptions.ReadTimeout:
                    print("\n\t\tError, trying again...")
                    time.sleep(3)

    game_log_df = pd.DataFrame(columns=HEADERS, data=game_logs)

    game_log_df["TEAM"] = game_log_df["MATCHUP"].apply(lambda x: x.split()[0])
    game_log_df["OPPONENT"] = game_log_df["MATCHUP"].apply(lambda x: x.split()[-1])
    game_log_df["HOME/AWAY"] = game_log_df["MATCHUP"].apply(
        lambda x: "AWAY" if x.split()[1] == "@" else "HOME"
    )

    game_log_df = game_log_df.drop(columns=["VIDEO_AVAILABLE", "MATCHUP"])

    player_map = players.set_index("PERSON_ID")

    game_log_df["player_name"] = game_log_df["Player_ID"].map(
        player_map["DISPLAY_FIRST_LAST"].to_dict()
    )

    game_log_df.sort_values(by=["GAME_DATE", "TEAM"], ascending=False, inplace=True)

    game_log_df.reset_index(drop=True, inplace=True)

    return game_log_df


def main():
    # The 3pt line was introduced into the NBA in the 1979 season
    
    seasons = [n for n in range(1979, 2025)]
    number_of_batches = math.ceil(len(seasons) / 5)

    for x in range(0, number_of_batches):
        season_batch = seasons[(x * 5) :]
        if len(season_batch) >= 5:
            season_batch = season_batch[:5]
        print(f"\nGrabbing gamelogs for {season_batch} seasons...\n")

        gamelog_df = scrape_game_logs(PLAYER_DF, season_batch)
        sql.export_df_to_sql(
            gamelog_df,
            table_name="player_gamelogs_v2",
            schema="gamelogs",
            behavior="append",
        )


if __name__ == "__main__":
    main()
