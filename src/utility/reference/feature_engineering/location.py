import pandas as pd
from utility.reference.reference import TEAMS

def get_game_logs() -> pd.DataFrame:
    file_path = input("Enter gamelogs filepath: ")
    return pd.read_csv(file_path)

def find_city(home_away):
    if home_away == "HOME":
        return TEAMS["TEAM"]["city"]
    else:
        return TEAMS["OPPONENT"]["city"]


def main():
    gamelog_df = get_game_logs()
    
    locations = []
    for n, row in gamelog_df.iterrows():
        home_away = row["HOME/AWAY"]
        locations.append(find_city(home_away))
    
    gamelog_df.to_csv("2020_2023gamelogs.csv", index=False)

if __name__ == "__main__":
    main()
            