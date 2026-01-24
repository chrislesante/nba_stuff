from utility.lines_model.datamodel import LinesAnalyzer
from dotenv import load_dotenv
import pandas as pd
import os
import json
import requests

load_dotenv()

pd.options.display.max_rows = 999
pd.options.display.max_columns = 999


LINES_ENDPOINT = "https://www.rotowire.com/betting/nba/tables/games-archive.php"
TODAYS_LINES = {
    "lines_endpoint": "https://www.rotowire.com/betting/nba/tables/nba-games.php?",
    "columns": [
        "gameID",
        "gameDate",
        "gameDay",
        "abbr",
        "oppAbbr",
        "best_moneylineBook",
        "best_moneyline",
        "best_spreadBook",
        "best_spread",
        "best_spreadML",
        "best_ouBook",
        "best_ou",
        "best_ouML",
    ],
}


def get_lines_raw_data_from_web(historical: bool = True) -> pd.DataFrame:
    if historical:
        print("\nGrabbing historical lines data...")
        lines_end_point = LINES_ENDPOINT
    else:
        print("\nGrabbing todays lines...")
        lines_end_point = TODAYS_LINES["lines_endpoint"]

    lines_request = requests.get(lines_end_point)
    lines_json = json.loads(lines_request.content)
    lines_df = pd.DataFrame.from_records(lines_json)

    if (not historical) and (not lines_df.empty):
        lines_df = lines_df[TODAYS_LINES["columns"]]

    return lines_df


def find_underdog(lines_df):
    underdogs = []
    for n, row in lines_df.iterrows():
        if row["favorite"] == row["home_team_abbrev"]:
            underdog = row["visit_team_abbrev"]
        else:
            underdog = row["home_team_abbrev"]

        underdogs.append(underdog)

    return underdogs


def process_lines_data(historical_lines: pd.DataFrame, todays_lines: pd.DataFrame):

    historical_lines["underdog"] = find_underdog(historical_lines)

    print("\nAnalyzing data...\n")
    lines = LinesAnalyzer(historical_lines, todays_lines)

    return lines


def main():
    again = "y"

    historical_lines = get_lines_raw_data_from_web()
    todays_lines = get_lines_raw_data_from_web(historical=False)

    lines = process_lines_data(historical_lines, todays_lines)

    lines.update_sql_table()

    if os.getenv("env") != 'local':
        again = 'n'

    while again.upper() == "Y":
        methods = {
                "Get today's lines": lines.get_todays_lines,
                "Tell me who to pick": lines.choose_picks,
                "Get Coverage Report": lines.get_coverage_report,
                "Get favorite splits": lines.get_favorite_splits,
                "Get underdog splits": lines.get_underdog_splits,
                "Get over/under splits": lines.get_over_under_splits,
                "Export all reports": lines.export_data,
                "Export Tables as HTML": lines.export_as_html,
                "Get new coverage summary": lines.get_new_coverage_summary,
                "Exit": None,
            }

        selection_dict = lines.create_selection_dict(methods)
        valid_selections = selection_dict.keys()
        choice = input("\nSelect an option: ")

        if (choice not in valid_selections):
            print("\nPlease select a valid selection\n")
        elif selection_dict[choice] == "Exit":
            confirm = input("\nAre you sure? (y/return) ")
            if confirm.upper() == "Y":
                lines.print_separator()
                print("\nGoodbye!\n")
                lines.print_separator()
                break
        else:
            methods[selection_dict[choice]]()


if __name__ == "__main__":
    main()
