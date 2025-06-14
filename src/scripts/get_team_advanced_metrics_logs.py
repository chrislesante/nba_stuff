from nba_api.stats.endpoints.boxscoreadvancedv3 import BoxScoreAdvancedV3 as bsa
from utility.reference import sql
import pandas as pd
import time
import math
import json
import requests
import re


def to_snake_case(name):
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def get_advanced_metrics(game_metadata):

    print("\nGrabbing new advanced metrics data...")
    records = []
    for n, row in enumerate(game_metadata):
        for attempt in range(0, 5):
            print(
                f"\n\tGetting advanced metrics data for {row['game_id']} - {n + 1} of {len(game_metadata)}"
            )
            #     for period in range(0, 5):
            #         period = str(period)
            kwargs = {
                "game_id": row["game_id"]
            }  # , "start_period": period, "end_period": period}
            try:
                try:
                    response = bsa(**kwargs).get_dict()["boxScoreAdvanced"]
                except:
                    response = bsa(f"00{row['game_id']}").get_dict()["boxScoreAdvanced"]

                enum = {"home": "homeTeam", "away": "awayTeam"}

                record = {
                    "game_id": response["gameId"],
                    "away_team_id": response["awayTeamId"],
                    "home_team_id": response["homeTeamId"],
                    "date": row["date"],
                    "season": int(row["season"]),
                    # "period": period
                }
                for team in enum:
                    stats = response[enum[team]]["statistics"]
                    for key in stats:
                        record[f"{team}_{key}"] = stats[key]

                records.append(record)
                time.sleep(0.6)
                break
            except json.decoder.JSONDecodeError:
                print("\n\t\tError, trying again...")
                time.sleep(1)
            except requests.exceptions.ReadTimeout:
                print("\n\t\tError, trying again...")
                time.sleep(3)
            except IndexError:
                print("\n\t\tError, trying again...")
                time.sleep(1)

    df = pd.DataFrame.from_records(records)
    df.rename({x: to_snake_case(x) for x in df.columns}, axis=1, inplace=True)

    return df


def get_game_metadata_from_player_gamelogs_traditional():
    query = """
        SELECT DISTINCT
            RIGHT("SEASON_ID", 4) as "season",
            "GAME_DATE" as "date",
            "Game_ID" as "game_id"
        FROM nba_gamelogs.player_gamelogs
        WHERE RIGHT("SEASON_ID", 4)::numeric >= 1996
        ORDER BY "GAME_DATE" DESC;
    """

    return sql.convert_sql_to_df(query=query)


def get_current_game_ids():
    query = """
    SELECT DISTINCT("game_id")
    FROM nba_gamelogs.team_advanced_metrics;
    """
    return sql.convert_sql_to_df(query=query)


def main():
    game_metadata = get_game_metadata_from_player_gamelogs_traditional()
    current_game_ids = get_current_game_ids()

    new_games = game_metadata.loc[
        ~(game_metadata["game_id"].isin(current_game_ids["game_id"]))
    ]

    new_games = new_games.to_dict(orient="records")

    number_of_batches = math.ceil(len(new_games) / 100)

    for x in range(0, number_of_batches):
        game_batch = new_games[(x * 100) :]
        if len(game_batch) >= 100:
            game_batch = game_batch[:100]
        print(
            f"\nGrabbing advanced metrics for batch {x} out of {number_of_batches}...\n"
        )

        export = get_advanced_metrics(game_batch)

        sql.export_df_to_sql(
            df=export,
            table_name="team_advanced_metrics",
            schema="nba_gamelogs",
            behavior="append",
        )


if __name__ == "__main__":
    main()
