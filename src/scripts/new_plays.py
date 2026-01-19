from utility.reference import sql
from nba_api.stats.endpoints import playbyplayv3 as pp
import pandas as pd
import numpy as np
import time
import json
import requests
import math

COLUMNS = [
    "gameId",
    "actionNumber",
    "clock",
    "period",
    "teamId",
    "teamTricode",
    "personId",
    "playerName",
    "playerNameI",
    "xLegacy",
    "yLegacy",
    "shotDistance",
    "shotResult",
    "isFieldGoal",
    "scoreHome",
    "scoreAway",
    "pointsTotal",
    "location",
    "description",
    "actionType",
    "subType",
    "videoAvailable",
    "shotValue",
    "actionId",
]

HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Referer": "https://www.nba.com",
    "Origin": "https://www.nba.com",
    "Accept": "application/json, text/plain, */*",
    "x-nba-stats-origin": "stats",
    "Connection": "keep-alive",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US;q=0.9,en;q=0.7",
}


def get_play_by_play_data(game_batch, columns):

    staging_df = pd.DataFrame(columns=columns)

    print("\nGrabbing new play by play data...")

    for n, row in enumerate(game_batch):
        for attempt in range(0, 5):
            print(
                f"\n\tGetting playbyplay data for {row['Game_ID']} - {n + 1} of {len(game_batch)}"
            )
            try:
                try:
                    play = pp.PlayByPlayV3(game_id=row['Game_ID'], headers=HTTP_HEADERS)
                except IndexError:
                    play = pp.PlayByPlayV3(game_id=f"00{row['Game_ID']}", headers=HTTP_HEADERS)

                new_plays = pd.DataFrame(
                            columns=columns, data=play.play_by_play.get_dict()["data"]
                        )
                new_plays['GAME_DATE'] = row['GAME_DATE']
                new_plays['SEASON'] = row['SEASON']
                new_plays.drop(columns='videoAvailable', inplace=True)

                staging_df = pd.concat(
                    [
                        staging_df,
                        new_plays,
                    ]
                )
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

    return staging_df

def lambda_handler(event, context):
    main()
    
def main():
    print("\nLoading gamelogs df...")

    # the nba only began tracking play by play data in 1996

    log_query = """
                SELECT 
                    DISTINCT("Game_ID"), 
                    "GAME_DATE", 
                    RIGHT("SEASON_ID", 4)::numeric AS "SEASON"
                FROM nba_gamelogs.player_gamelogs
                WHERE CAST("SEASON_ID" AS INTEGER) >= 21996
                """

    logs = sql.convert_sql_to_df(query=log_query)

    logs["Game_ID"] = logs["Game_ID"].apply(lambda x: str(x).lstrip("00"))

    game_ids = logs["Game_ID"].unique()

    print("\nLoading play by play df...")

    pbp_query = 'SELECT DISTINCT("gameId") FROM nba_gamelogs.play_by_play;'
    try:
        play_by_play = sql.convert_sql_to_df(query=pbp_query)

        play_by_play["gameId"] = play_by_play["gameId"].apply(lambda x: str(x).lstrip("00"))
        play_by_play_games = play_by_play["gameId"].unique()

        del play_by_play

        new_ids = [x for x in game_ids if x not in play_by_play_games]
    except Exception as e:
        print(f'\nAn error has occurred {e}')
        new_ids = game_ids

    print(f"\n{len(new_ids)} new game ids found!")

    games = logs.loc[logs["Game_ID"].isin(new_ids)].to_dict(orient='records')

    del logs

    number_of_batches = math.ceil(len(games) / 100)

    for x in range(0, number_of_batches):
        game_batch = games[(x * 100) :]
        if len(game_batch) >= 100:
            game_batch = game_batch[:100]
        print(f"\nGrabbing plays for batch {x} out of {number_of_batches}...\n")

        export = get_play_by_play_data(game_batch, COLUMNS)
        export = export.replace("", np.nan)
        sql.export_df_to_sql(
            df=export, table_name="play_by_play", schema="nba_gamelogs", behavior="append"
        )


if __name__ == "__main__":
    main()
