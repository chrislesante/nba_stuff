import pandas as pd
from utility.reference import sql

def get_restore_file():
    path = input("\nEnter flatfile path: ")
    return pd.read_csv(path)

def main():
    revert_df = get_restore_file()
    sql.export_df_to_sql(df=revert_df, table_name="player_gamelogs_v2", schema="gamelogs", behavior="replace")
    print("\nGamelogs reverted.")

if __name__ == "__main__":
    main()
