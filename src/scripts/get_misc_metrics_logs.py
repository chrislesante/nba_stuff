from nba_api.stats.endpoints.boxscoremiscv3 import BoxScoreMiscV3 as bsm
from utility.reference import sql
from psycopg.errors import UndefinedColumn, ProgrammingError
import pandas as pd
import time
import math
import json
import requests
import re


def to_snake_case(name):
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def get_misc_metrics(game_metadata):

    print("\nGrabbing new misc metrics data...")
    team_records = []
    player_records = []
    for n, row in enumerate(game_metadata):
        for attempt in range(0, 5):
            print(
                f"\n\tGetting misc metrics data for {row['game_id']} - {n + 1} of {len(game_metadata)}"
            )
            #     for period in range(0, 5):
            #         period = str(period)
            kwargs = {
                "game_id": row["game_id"]
            }  # , "start_period": period, "end_period": period}
            try:
                try:
                    response = bsm(**kwargs).get_dict()["boxScoreMisc"]
                except:
                    response = bsm(f"00{row['game_id']}").get_dict()["boxScoreMisc"]

                enum = {"home": "homeTeam", "away": "awayTeam"}

                team_record = {
                    "game_id": response["gameId"],
                    "away_team_id": response["awayTeamId"],
                    "home_team_id": response["homeTeamId"],
                    "date": row["date"],
                    "season": int(row["season"]),
                    # "period": period
                }

                # parse team data

                for team in enum:
                    stats = response[enum[team]]["statistics"]
                    for key in stats:
                        team_record[f"{team}_{key}"] = stats[key]

                    # parse player_data

                    players = response[enum[team]]["players"]
                    for player in players:
                        player_record = {
                            "game_id": response["gameId"],
                            "date": row["date"],
                            "season": int(row["season"]),
                            "team_id": response[enum[team]]["teamId"],
                            # "period": period
                        }
                        player_record["home_away"] = team
                        player_record["player_id"] = player["personId"]
                        player_record["player_slug"] = player["playerSlug"]
                        player_record.update(player["statistics"])

                        player_records.append(player_record)

                team_records.append(team_record)

                time.sleep(0.6)
                break
            except json.decoder.JSONDecodeError as e:
                print(f"\n\t\tError: {e}, trying again...")
                time.sleep(1)
            except requests.exceptions.ReadTimeout as e:
                print(f"\n\t\tError: {e}, trying again...")
                time.sleep(3)
            except IndexError as e:
                print(f"\n\t\tError: {e}, trying again...")
                time.sleep(1)
            except AttributeError as e:
                print(f"\n\t\tError: {e}, trying again...")
                time.sleep(1)

    team_df = pd.DataFrame.from_records(team_records)
    team_df.rename({x: to_snake_case(x) for x in team_df.columns}, axis=1, inplace=True)

    player_df = pd.DataFrame.from_records(player_records)
    player_df.rename(
        {x: to_snake_case(x) for x in team_df.columns}, axis=1, inplace=True
    )

    return team_df, player_df


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
    FROM nba_gamelogs.team_misc_metrics;
    """
    return sql.convert_sql_to_df(query=query)


def strip_leading_zeroes(string):
    return string.lstrip("0")


def main():
    game_metadata = get_game_metadata_from_player_gamelogs_traditional()

    current_game_ids = get_current_game_ids()

    # current_game_ids = pd.DataFrame(columns=["game_id"], data=[""])

    for df in [game_metadata, current_game_ids]:
        df["game_id"] = df["game_id"].apply(strip_leading_zeroes)

    new_games = game_metadata.loc[
        ~(game_metadata["game_id"].isin(current_game_ids["game_id"]))
    ]

    new_games = new_games.to_dict(orient="records")

    number_of_batches = math.ceil(len(new_games) / 100)

    for x in range(0, number_of_batches):
        game_batch = new_games[(x * 100) :]
        if len(game_batch) >= 100:
            game_batch = game_batch[:100]
        print(f"\nGrabbing misc metrics for batch {x} out of {number_of_batches}...\n")

        team_df, player_df = get_misc_metrics(game_batch)

        behavior = "append" # if x == 0 else "replace"
        sql.export_df_to_sql(
            df=team_df,
            table_name="team_misc_metrics",
            schema="nba_gamelogs",
            behavior=behavior,
        )

        sql.export_df_to_sql(
            df=player_df,
            table_name="player_misc_metrics",
            schema="nba_gamelogs",
            behavior=behavior,
        )


if __name__ == "__main__":
    main()
