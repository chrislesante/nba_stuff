import pandas as pd

STANDINGS_TRACKER_COLUMNS = ["season", "date", "team", "win", "loss"]


def get_game_logs():
    game_logs_df = pd.read_csv(
    "/Users/christianlesante/Desktop/github/nba_stuff/src/utility/reference/nba_gamelogs_2020_to_2023_seasons.csv"
)
    return game_logs_df

def win_or_loss(wl: str):
    if wl[0].upper() == "W":
        return 1
    else:
        return 0

def create_standings_tracker_df():
    standings_tracker = pd.DataFrame(columns=STANDINGS_TRACKER_COLUMNS)

    return standings_tracker

def get_games_at_date(date, game_logs_df) -> pd.DataFrame:
    return game_logs_df[game_logs_df["GAME_DATE"] == date].copy()

def group_games_by_team(games):
    return games.groupby(by=["GAME_DATE","TEAM"], as_index=False).sum()

def get_season_of_games(games):
    return games["SEASON_ID"].unique()[0]

def add_win_loss_columns(teams_grouped):
    teams_grouped["loss"] = teams_grouped["win"].apply(win_or_loss)
    teams_grouped["win"] = teams_grouped["win"].apply(win_or_loss)
    teams_grouped = teams_grouped[STANDINGS_TRACKER_COLUMNS]

    return teams_grouped

def cumulate_win_loss(standings_tracker, teams_grouped):
    if len(standings_tracker) == 0:
        standings_tracker = pd.concat([standings_tracker, teams_grouped])

    else:
        merged = teams_grouped.merge(standings_tracker, on=["season","team", "date"], how="right")
        merged["win"] = merged["win_x"] + merged["win_y"]
        merged["loss"] = merged["loss_x"] + merged["loss_y"]
        merged = merged[STANDINGS_TRACKER_COLUMNS]
        standings_tracker = pd.concat([standings_tracker, merged])
   
    return standings_tracker

def main():
    game_logs_df = get_game_logs()
    standings_tracker = create_standings_tracker_df()

    dates = game_logs_df["GAME_DATE"].unique()

    for date in dates:
        games = get_games_at_date(date, game_logs_df)
        teams_grouped = group_games_by_team(games)
        season = get_season_of_games(games)

        teams_grouped.rename(
            {"SEASON_ID": "season", "GAME_DATE": "date", "TEAM": "team", "WL": "win"},
            axis=1,
            inplace=True,
        )

        teams_grouped = add_win_loss_columns(teams_grouped)

        teams_grouped["season"] = season

        standings_tracker = cumulate_win_loss(standings_tracker, teams_grouped)
    
    standings_tracker.to_csv("moving_standings_test.csv",index = False)


if __name__ == "__main__":
    main()
