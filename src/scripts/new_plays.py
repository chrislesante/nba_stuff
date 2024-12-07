from utility.reference import sql
from nba_api.stats.endpoints import playbyplayv3 as pp
import pandas as pd
import time
import json
import requests

def get_play_by_play_data():
    print("\nLoading gamelogs df...")
    logs = sql.convert_sql_to_df(table_name="player_gamelogs", schema="gamelogs")

    print("\nLoading play by play df...")
    play_by_play = sql.convert_sql_to_df(table_name="play_by_play", schema="gamelogs")

    game_ids = logs["Game_ID"].unique()
    play_by_play["gameId"] = play_by_play["gameId"].apply(lambda x: str(x).lstrip("00"))
    play_by_play_games = play_by_play["gameId"].unique()

    new_ids = [x for x in game_ids if x not in play_by_play_games]

    print("\nGrabbing new play by play data...")
    
    for n, id_ in enumerate(new_ids):
        for attempt in range(0, 5):
            print(f"\n\tGetting playbyplay data for {id_}")
            try:
                try:
                    play = pp.PlayByPlayV3(game_id=id_)
                except IndexError:
                    play = pp.PlayByPlayV3(game_id=f"00{id_}")

                df = pd.DataFrame(columns=play.play_by_play.get_dict()["headers"], data=play.play_by_play.get_dict()["data"])
                play_by_play = pd.concat([play_by_play, df])
                time.sleep(0.3)
                break
            except json.decoder.JSONDecodeError:
                print("\n\t\tError, trying again...")
                time.sleep(0.5)
            except requests.exceptions.ReadTimeout:
                print("\n\t\tError, trying again...")
                time.sleep(3)
    
    return play_by_play

def main():
    export = get_play_by_play_data()
    sql.export_df_to_sql(df=export, table_name="play_by_play", schema="gamelogs", behavior="replace")

if __name__ == "__main__":
    main()