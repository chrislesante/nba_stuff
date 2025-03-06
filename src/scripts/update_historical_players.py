from nba_api.stats.endpoints import CommonPlayerInfo
from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog
from utility.reference import sql
import pandas as pd
from datetime import date
import time
import os

TODAY = date.today()


def fetch_latest_data():
    player_dict = players.get_players()

    common_player_info_df = pd.DataFrame(
        columns=CommonPlayerInfo(player_dict[0]["id"]).get_dict()["resultSets"][0][
            "headers"
        ]
    )

    for player in player_dict:
        for attempt in range(1, 5):
            try:
                print(f"\n\tGrabbing {player['full_name']} data")
                row = CommonPlayerInfo(player["id"]).get_dict()["resultSets"][0][
                    "rowSet"
                ][0]
                length = len(common_player_info_df)
                common_player_info_df.loc[length] = row
                time.sleep(0.200)
                break
            except Exception:
                print("\nTimeout error. Trying again...")
                time.sleep(0.2 * attempt)

    return common_player_info_df


def get_current_data():
    return sql.convert_sql_to_df(table_name="all_historical_players", schema="general")


def export_flatfiles(old_df, new_df):
    download_path = f"{sql.FLATFILE_PATH}old_historical_players/{TODAY}"
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    old_df.to_csv(f"{download_path}/old_players_df.csv", index=False)

    download_path = f"{sql.FLATFILE_PATH}new_historical_players/{TODAY}"
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    new_df.to_csv(f"{download_path}/new_players_df.csv", index=False)

    print(f"\nFlatfiles exported to {sql.FLATFILE_PATH}")


def fix_dtypes(new_df):
    new_df["DRAFT_YEAR"] = new_df["DRAFT_YEAR"].map({"Undrafted": 0})
    new_df["DRAFT_ROUND"] = new_df["DRAFT_ROUND"].fillna(0)
    new_df["DRAFT_NUMBER"] = new_df["DRAFT_NUMBER"].fillna(0)

    new_df["DRAFT_YEAR"] = new_df["DRAFT_YEAR"].astype(int)
    new_df["DRAFT_ROUND"] = new_df["DRAFT_ROUND"].astype(int)
    new_df["DRAFT_NUMBER"] = new_df["DRAFT_NUMBER"].astype(int)

    return new_df


def main():
    old_df = get_current_data()
    new_df = fetch_latest_data()
    # new_df = fix_dtypes(new_df)
    export_flatfiles(old_df, new_df)
    sql.export_df_to_sql(
        df=new_df,
        table_name="all_historical_players",
        schema="general",
        behavior="replace",
    )


if __name__ == "__main__":
    main()
