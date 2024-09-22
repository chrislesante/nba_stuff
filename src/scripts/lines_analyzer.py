from utility.lines_model.datamodel import LinesAnalyzer
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

    return filter_df


def get_lines_raw_data_from_web(league: str):
    print("\nGrabbing raw lines data...")
    lines_end_point = LINES_ENDPOINTS[league]
    lines_request = requests.get(lines_end_point)
    lines_json = json.loads(lines_request.content)
    lines_df = pd.DataFrame.from_records(lines_json)

    return lines_df


def get_lines_raw_data_from_local_file():
    lines_path = input("\nEnter lines csv data file path: ")
    print("\nGrabbing raw lines data...")
    lines_df = pd.read_csv(lines_path)

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


def export_data(lines_obj):
    lines_obj.coverage_summary.sort_values(by="team", inplace=True)
    lines_obj.coverage_summary.to_csv("coverage.csv", index=False)
    lines_obj.underdog_split.to_csv("underdog_split.csv", index=False)
    lines_obj.favorite_split.to_csv("favorite_split.csv", index=False)

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
    start_year = input("\nEnter start year: ")
    end_year = input("\nEnter end year: ")

    filter_df = filter_seasons(lines_df, start_year, end_year)

    filter_df["underdog"] = find_underdog(filter_df)

    print("\nAnalyzing data...\n")
    lines = LinesAnalyzer(filter_df)

    return lines


def get_coverage_report(lines: LinesAnalyzer):
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


def get_favorite_splits(lines: LinesAnalyzer):
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


def get_underdog_splits(lines: LinesAnalyzer):
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


def main():
    again = "Y"
    source = input("\nLocal file or web? (l/return) ")

    if source.upper() == "L":
        lines_df = get_lines_raw_data_from_local_file()
    else:
        selection_dict = create_selection_dict(LINES_ENDPOINTS.keys())
        league = selection_dict[input("\nSelect a league to analyze: ")]
        lines_df = get_lines_raw_data_from_web(league)
        
    lines = process_lines_data(lines_df)

    while again.upper() == "Y":
        methods = [
            "Get Coverage Report",
            "Get favorite splits",
            "Get underdog splits",
            "Export all reports",
            "Change years",
        ]
        selection_dict = create_selection_dict(methods)
        choice = input("\nSelect an option: ")

        if selection_dict[choice] == "Get Coverage Report":
            get_coverage_report(lines)
        elif selection_dict[choice] == "Get favorite splits":
            get_favorite_splits(lines)
        elif selection_dict[choice] == "Get underdog splits":
            get_underdog_splits(lines)
        elif selection_dict[choice] == "Export all reports":
            export_data(lines)
        elif selection_dict[choice] == "Change years":
            lines = process_lines_data(lines_df)


if __name__ == "__main__":
    main()