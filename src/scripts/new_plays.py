from utility.reference import sql
from nba_api.stats.endpoints import playbyplayv3 as pp
import pandas as pd
import time
import json
import requests
import math

def get_play_by_play_data(game_batch):

    print("\nGrabbing new play by play data...")
    
    for n, id_ in enumerate(game_batch):
        for attempt in range(0, 5):
            print(f"\n\tGetting playbyplay data for {id_} - {n + 1} of {len(game_batch)}")
            try:
                try:
                    play = pp.PlayByPlayV3(game_id=id_)
                except IndexError:
                    play = pp.PlayByPlayV3(game_id=f"00{id_}")

                df = pd.DataFrame(columns=play.play_by_play.get_dict()["headers"], data=play.play_by_play.get_dict()["data"])
                time.sleep(0.3)
                break
            except json.decoder.JSONDecodeError:
                print("\n\t\tError, trying again...")
                time.sleep(1)
            except requests.exceptions.ReadTimeout:
                print("\n\t\tError, trying again...")
                time.sleep(3)

    return df

def main():
    print("\nLoading gamelogs df...")
    logs = sql.convert_sql_to_df(table_name="player_gamelogs_v2", schema="gamelogs")

    # print("\nLoading play by play df...")
    # play_by_play = sql.convert_sql_to_df(table_name="play_by_play", schema="gamelogs")

    game_ids = logs["Game_ID"].apply(lambda x: str(x).lstrip("00")).unique()
    # play_by_play["gameId"] = play_by_play["gameId"].apply(lambda x: str(x).lstrip("00"))
    # play_by_play_games = play_by_play["gameId"].unique()

    play_by_play_games = []

    
    # del play_by_play
    del logs

    new_ids = [x for x in game_ids if x not in play_by_play_games]

    print(f"\n{len(new_ids)} new game ids found!")

    number_of_batches = math.ceil(len(new_ids) / 100)

    for x in range(0, number_of_batches):
        game_batch = new_ids[(x * 100) :]
        if len(game_batch) >= 100:
            game_batch = game_batch[:100]
        print(f"\nGrabbing plays for batch {x} out of {math.floor(len(new_ids) / 100)}...\n")

        export = get_play_by_play_data(game_batch)
        sql.export_df_to_sql(df=export, table_name="play_by_play", schema="gamelogs", behavior="append")

if __name__ == "__main__":
    main()