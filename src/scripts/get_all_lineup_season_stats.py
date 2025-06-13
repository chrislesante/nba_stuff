from nba_api.stats.endpoints.leaguedashlineups import LeagueDashLineups as ldl
from utility.reference import sql
from datetime import datetime as dt
import pandas as pd
import time

CURRENT_SEASON = dt.today().year


def get_lineup_data():
    for n in range(2007, CURRENT_SEASON):
        print(f"\nGrabbing lineup metrics for the {str(n)} season...")
        for x in range(0, 5):
            print(f"\n\tGrabbing period {str(x)} data...")
            args = {
                "season": f"{str(n)}-{str(n + 1)[-2:]}",
                "per_mode_detailed": "Totals",
                "period": str(x),
            }
            lineups = ldl(**args)
            df = pd.DataFrame(
                columns=lineups.get_dict()["resultSets"][0]["headers"],
                data=lineups.get_dict()["resultSets"][0]["rowSet"],
            )
            df["GROUP_ID"] = df["GROUP_ID"].apply(lambda x: x.strip("-").split("-"))

            df.drop("GROUP_SET", axis=1, inplace=True)

            for column in df.columns:
                if "RANK" in column:
                    df.drop(column, axis=1, inplace=True)
                else:
                    df.rename({column: column.lower()}, axis=1, inplace=True)

            df["season"] = n
            df["period"] = x

            behavior = "replace" if n == 2007 else "append"

            sql.export_df_to_sql(
                df=df,
                table_name="lineup_totals",
                schema="nba_season_metrics",
                behavior=behavior,
            )

def main():
    get_lineup_data()


if __name__ == "__main__":
    main()
