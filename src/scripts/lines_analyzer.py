from utility.lines_model.datamodel import LinesAnalyzer
from utility.reference import sql
import pandas as pd
import json
import requests

pd.options.display.max_rows = 999
pd.options.display.max_columns = 999


LINES_ENDPOINTS = {
    "NBA": "https://www.rotowire.com/betting/nba/tables/games-archive.php",
    "NFL": "https://www.rotowire.com/betting/nfl/tables/games-archive.php",
    "MLB": "https://www.rotowire.com/betting/mlb/tables/games-archive.php",
}

TODAYS_LINES = {
    "NBA": "https://www.rotowire.com/betting/nba/tables/nba-games.php?",
    "keep_columns": [
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


def filter_seasons(lines_df, start_year, end_year):
    filter_df = lines_df.copy()
    filter_df["season"] = filter_df["season"].apply(lambda x: int(x))
    first_season = filter_df["season"].min()
    last_season = filter_df["season"].max()
    valid_seasons = filter_df["season"].unique()

    if start_year.isdigit():
        start_year = int(start_year)
    if start_year not in valid_seasons:
        start_year = first_season

    if end_year.isdigit():
        end_year = int(end_year)
    if end_year not in valid_seasons:
        end_year = last_season

    if start_year > end_year:
        start = end_year
        end = start_year
        start_year = start
        end_year = end

    filter_df = filter_df[
        (filter_df["season"] >= start_year) & (filter_df["season"] <= end_year)
    ]

    return filter_df, start_year, end_year


def get_lines_raw_data_from_web(league: str, historical: bool = True) -> pd.DataFrame:
    if historical:
        print("\nGrabbing historical lines data...")
        lines_end_point = LINES_ENDPOINTS[league]
    else:
        print("\nGrabbing todays lines...")
        lines_end_point = TODAYS_LINES[league]

    lines_request = requests.get(lines_end_point)
    lines_json = json.loads(lines_request.content)
    lines_df = pd.DataFrame.from_records(lines_json)

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


def export_data(lines_obj: LinesAnalyzer, todays_lines: pd.DataFrame):
    lines_obj.coverage_summary.sort_values(by="team", inplace=True)
    lines_obj.coverage_summary.to_csv("coverage.csv", index=False)
    lines_obj.underdog_split.to_csv("underdog_split.csv", index=False)
    lines_obj.favorite_split.to_csv("favorite_split.csv", index=False)
    lines_obj.over_under_splits.to_csv("over_under_splits.csv", index=False)
    todays_lines.to_csv("todays_lines.csv", index=False)

    print("\nSummaries exported successfully!\n")


def create_selection_dict(methods: list) -> dict:
    selection_dict = {}
    print()
    for n, method in enumerate(methods):
        choice = str(n + 1)
        selection_dict[choice] = method
        print(choice + ". " + method)

    return selection_dict


def process_lines_data(lines_df: pd.DataFrame):

    lines_df["underdog"] = find_underdog(lines_df)

    print("\nAnalyzing data...\n")
    lines = LinesAnalyzer(lines_df)

    return lines


def get_coverage_report(lines: LinesAnalyzer):
    table_header()
    methods = ["fav_hit_percentage", "dog_hit_percentage", "overall_hit_percentage"]
    selection_dict = create_selection_dict(methods)
    choice = input("\nSelect an option to sort by: ")
    descending = input("\nDescending or ascending order (d/return): ")
    if descending.upper() == "D":
        print(
            lines.coverage_summary.sort_values(
                by=selection_dict[choice], ascending=False
            )
        )
    else:
        print(lines.coverage_summary.sort_values(by=selection_dict[choice]))

    table_header()


def get_favorite_splits(lines: LinesAnalyzer):
    table_header()
    methods = ["hit_percentage_as_favorite_away", "hit_percentage_as_favorite_home"]
    selection_dict = create_selection_dict(methods)
    choice = input("\nSelect an option to sort by: ")
    descending = input("\nDescending or ascending order (d/return): ")
    if descending.upper() == "D":
        print(
            lines.favorite_split.sort_values(by=selection_dict[choice], ascending=False)
        )
    else:
        print(lines.favorite_split.sort_values(by=selection_dict[choice]))

    table_header()


def get_underdog_splits(lines: LinesAnalyzer):
    table_header()
    methods = ["hit_percentage_as_underdog_away", "hit_percentage_as_underdog_home"]
    selection_dict = create_selection_dict(methods)
    choice = input("\nSelect an option to sort by: ")
    descending = input("\nDescending or ascending order (d/return): ")
    if descending.upper() == "D":
        print(
            lines.underdog_split.sort_values(by=selection_dict[choice], ascending=False)
        )
    else:
        print(lines.underdog_split.sort_values(by=selection_dict[choice]))

    table_header()


def get_over_under_splits(lines: LinesAnalyzer):
    table_header()
    filter_methods = ["over_hit_home_percentage", "over_hit_away_percentage"]
    selection_dict = create_selection_dict(filter_methods)
    choice = input("\nSelect an option to sort by: ")
    descending = input("\nDescending or ascending order (d/return): ")
    if descending.upper() == "D":
        print(
            lines.over_under_splits.sort_values(
                by=selection_dict[choice], ascending=False
            )
        )
    else:
        print(lines.over_under_splits.sort_values(by=selection_dict[choice]))

    table_header()


def table_header():
    print("\n**********************************************************\n")


def choose_picks(todays_lines: pd.DataFrame):
    table_header()
    print("This option is not yet implemented.")
    table_header()


def export_html(lines: LinesAnalyzer, todays_lines: pd.DataFrame):
    html_string = (
        """
                    <!DOCTYPE html>
                    <html>
                    <head>
                    <title>DataFrames</title>
                    </head>
                    <body>
                    <h1>Coverage Summary</h1>
                    """
        + lines.coverage_summary.to_html()
        + """
                    <h1>Underdog Splits</h1>
                    """
        + lines.underdog_split.to_html()
        + """
                    <h1>Favorite Splits</h1>
                    """
        + lines.favorite_split.to_html()
        + """
                    <h1>Over Under Splits</h1>
                    """
        + lines.over_under_splits.to_html()
        + """
                    <h1>Todays Lines</h1>
                    """
        + todays_lines.to_html()
        + """
                    </body>
                    </html>
                    """
    )

    with open("lines.html", "w") as f:
        f.write(html_string)


def update_sql_table(lines: LinesAnalyzer):
    sql.export_df_to_sql(lines.raw)


def get_new_coverage_report(lines, start_year, end_year):
    coverage_summary = LinesAnalyzer.get_new_coverage_summary(
        lines.raw, start_year, end_year
    )
    print(coverage_summary)
    coverage_summary.to_csv("new_coverage_report.csv", index=False)


def main():
    again = 'y'

    lines_df = get_lines_raw_data_from_web('NBA')

    lines = process_lines_data(lines_df)
    todays_lines = get_lines_raw_data_from_web('NBA', historical=False)
    try:
        todays_lines = todays_lines[TODAYS_LINES["keep_columns"]]
    except KeyError:
        todays_lines = "No lines left for the day."

    while again.upper() == "Y":
        methods = [
            "Get today's lines",
            "Tell me who to pick",
            "Get Coverage Report",
            "Get favorite splits",
            "Get underdog splits",
            "Get over/under splits",
            "Export all reports",
            "Export Tables as HTML",
            "Update Lines SQL table",
            "Get new coverage summary",
            "Change years",
            "Exit",
        ]
        selection_dict = create_selection_dict(methods)
        valid_selections = selection_dict.keys()
        choice = input("\nSelect an option: ")

        if choice not in valid_selections:
            print("\nPlease select a valid selection\n")
        elif selection_dict[choice] == "Get today's lines":
            table_header()
            print(todays_lines)
            table_header()
        elif selection_dict[choice] == "Get Coverage Report":
            get_coverage_report(lines)
        elif selection_dict[choice] == "Get favorite splits":
            get_favorite_splits(lines)
        elif selection_dict[choice] == "Get underdog splits":
            get_underdog_splits(lines)
        elif selection_dict[choice] == "Get over/under splits":
            get_over_under_splits(lines)
        elif selection_dict[choice] == "Export all reports":
            export_data(lines, todays_lines)
        elif selection_dict[choice] == "Tell me who to pick":
            picks = choose_picks(todays_lines)
            print(picks)
        elif selection_dict[choice] == "Export Tables as HTML":
            export_html(lines, todays_lines)
        elif selection_dict[choice] == "Update Lines SQL table":
            update_sql_table(lines)
        elif selection_dict[choice] == "Get new coverage summary":
            get_new_coverage_report(lines, start_year, end_year)
        elif selection_dict[choice] == "Change years":
            lines, start_year, end_year = process_lines_data(lines_df)
        elif selection_dict[choice] == "Exit":
            confirm = input("\nAre you sure? (y/return) ")
            if confirm.upper() == "Y":
                table_header()
                print("\nGoodbye!\n")
                table_header()
                break


if __name__ == "__main__":
    main()
