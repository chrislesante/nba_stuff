from utility.lines_model.datamodel import LinesAnalyzer
import pandas as pd
import json
import requests

LINES_ENDPOINTS = {
    "NBA": "https://www.rotowire.com/betting/nba/tables/games-archive.php",
    "NFL": "https://www.rotowire.com/betting/nfl/tables/games-archive.php",
    "MLB": "https://www.rotowire.com/betting/mlb/tables/games-archive.php"
}

def filter_seasons(lines_df, start_year, end_year):
    lines_df["season"] = lines_df["season"].apply(lambda x: int(x))
    first_season = lines_df["season"].min()
    last_season = lines_df["season"].max()
    valid_seasons = lines_df["season"].unique()

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
    
    lines_df = lines_df[(lines_df["season"] >= start_year) & (lines_df["season"] <= end_year)]

    return lines_df

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
    underdogs =[]
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
        print(choice + "." + method)

    
    return selection_dict

def main():
    start_year = input("\nEnter start year: ")
    end_year = input("\nEnter end year: ")

    source = input("\nLocal file or web? (l/return) ")

    if source.upper() == "L":
        lines_df = get_lines_raw_data_from_local_file()
    else:
        selection_dict = create_selection_dict(LINES_ENDPOINTS.keys())
        league = selection_dict[input("\nSelect a league to analyze: ")]
        lines_df = get_lines_raw_data_from_web(league)

    lines_df = filter_seasons(lines_df, start_year, end_year)

    lines_df["underdog"] = find_underdog(lines_df)

    print("\nAnalyzing data...\n")
    lines = LinesAnalyzer(lines_df)

    export = input("\nExport data? (y/return) ")
    if export.upper() == "Y":
        export_data(lines)


    # print(lines.raw)
    # print("\n\n\n")
    # print(lines.coverage_summary)
    # print("\n\n\n")
    # print(lines.underdog_split)
    # print("\n\n\n")
    # print(lines.favorite_split)
    # print("\n\n\n")
    # print(lines.get_away_data("underdog"))
    # print("\n\n\n")
    # print(lines.get_home_data("favorite"))


if __name__ == "__main__":
    main()



