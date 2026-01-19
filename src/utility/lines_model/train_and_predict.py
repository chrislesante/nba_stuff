import pandas as pd
import numpy as np
import json
import requests
import datetime as dt
from nba_api.stats.endpoints.leaguestandings import LeagueStandings as ls
from utility.reference import sql, injury_scraper as inj
from sklearn.linear_model import LinearRegression

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Referer": "google.com",
}


def get_x_y(training_data: pd.DataFrame, model, window_ngames: int = 3):
    training_data.replace(r"^\s*$", np.nan, regex=True, inplace=True)
    training_data.dropna(inplace=True)
    training_data = training_data.loc[
        (training_data["HOME_TEAM_GAMES_PLAYED"] >= 5)
        & (training_data["AWAY_TEAM_GAMES_PLAYED"] >= 5)
    ]

    predictors = [
        f"HOME_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG",
        f"HOME_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG_STDDEV",
        f"AWAY_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG",
        f"AWAY_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG_STDDEV",
        "HOME_ACTIVE_PLAYERS_SEASON_PPG",
        "HOME_ACTIVE_PLAYERS_SEASON_POINTS_STDDEV",
        "AWAY_ACTIVE_PLAYERS_SEASON_PPG",
        "AWAY_ACTIVE_PLAYERS_SEASON_POINTS_STDDEV",
        "HOME_TEAM_OPP_PPG",
        f"HOME_TEAM_OPP_LAST_{window_ngames}_PPG",
        "AWAY_TEAM_OPP_PPG",
        f"AWAY_TEAM_OPP_LAST_{window_ngames}_PPG",
        "HOME_TEAM_PPG_AT_HOME",
        "AWAY_TEAM_PPG_AWAY",
        f"HOME_TEAM_LAST_{window_ngames}_PPG_AT_HOME",
        f"AWAY_TEAM_LAST_{window_ngames}_PPG_AWAY",
        "HOME_TEAM_2ND_OF_B2B",
        "AWAY_TEAM_2ND_OF_B2B",
        "HOME_TEAM_GAMES_PLAYED",
        "HOME_TEAM_WIN_PCT",
        "AWAY_TEAM_GAMES_PLAYED",
        "AWAY_TEAM_WIN_PCT",
        "HOME_AVG_HEIGHT_INCHES",
        "HOME_STDDEV_HEIGHT_INCHES",
        "AWAY_AVG_HEIGHT_INCHES",
        "AWAY_STDDEV_HEIGHT_INCHES",
    ]

    if model == "ou":
        y = "GAME_TOTAL_PTS"
    elif model == "lines":
        y = "DIFF"

    return training_data[predictors], training_data[y]


def train_model(training_data, model):
    X, y = get_x_y(training_data, model)

    return LinearRegression().fit(X, y), X.columns


def get_todays_lineups():
    today = dt.date.today()
    year = str(today.year)
    month = today.month
    day = today.day

    if month < 10:
        month = "0" + str(month)
    if day < 10:
        day = "0" + str(day)

    player_columns = [
        "personId",
        "firstName",
        "lastName",
        "playerName",
        "lineupStatus",
        "position",
        "rosterStatus",
    ]
    team_columns = ["teamId", "teamAbbreviation"]

    lineups = requests.get(
        f"https://stats.nba.com/js/data/leaders/00_daily_lineups_{year}{month}{day}.json",
        headers=HEADERS,
    )

    staging_df = pd.DataFrame.from_records(json.loads(lineups.content)["games"])

    home_team_df = staging_df.drop("awayTeam", axis=1)
    away_team_df = staging_df.drop("homeTeam", axis=1)

    def explode_players(df, home_away: str):
        df["players"] = df[home_away].apply(lambda x: x["players"])
        for column in team_columns:
            df[column] = df[home_away].apply(lambda x: x[column])
        df.sort_values(by="teamId", inplace=True)
        df = df.explode("players")

        for column in player_columns:
            df[column] = df["players"].apply(lambda x: x[column])

        df["home_away"] = "home" if home_away[:4] == "home" else "away"

        return df.drop(columns=[home_away, "players"])

    home_team_df = explode_players(home_team_df, "homeTeam")
    away_team_df = explode_players(away_team_df, "awayTeam")

    lineup_df = pd.concat([home_team_df, away_team_df])

    lineup_df = lineup_df.drop(columns=["lineupStatus", "position"]).drop_duplicates()

    return lineup_df.loc[lineup_df["rosterStatus"] == "Active"]


def cross_ref_injury_report(lineups_df):
    injury_report = inj.grab_injury_report()

    return lineups_df.loc[
        ~(
            lineups_df["playerName"].isin(
                injury_report["Player"].loc[injury_report["Status"].str.contains("Out")]
            )
        )
    ]


def get_active_player_data():
    lineups_df = get_todays_lineups()
    active_players = cross_ref_injury_report(lineups_df)
    active_player_df = sql.agg_active_player_new_x_data(active_players)
    active_player_df["STDDEV_HEIGHT_INCHES"] = active_player_df["HEIGHT_INCHES"]

    return (
        active_player_df.drop(
            columns=["Player_ID", "player_name", "GAME_DATE", "SEASON_YEAR", "rn"]
        )
        .groupby("TEAM_ID", as_index=False)
        .agg(
            {
                "LAST_3_PPG": "sum",
                "LAST_3_PPG_STDDEV": "sum",
                "SEASON_PPG": "sum",
                "SEASON_PPG_STDDEV": "sum",
                "HEIGHT_INCHES": "mean",
                "STDDEV_HEIGHT_INCHES": "std",
            }
        )
    )


def filter_and_align_x_data(merged_df, todays_lines, window_ngames: int = 3):
    window_ngames = str(window_ngames)
    merged_columns_enum = {
        "SEASON_PPG": "ACTIVE_PLAYERS_SEASON_PPG",
        f"LAST_{window_ngames}_PPG": f"ACTIVE_PLAYERS_LAST_{window_ngames}_PPG",
        "SEASON_OPP_PPG": "TEAM_OPP_PPG",
        f"LAST_{window_ngames}_OPP_PPG": f"TEAM_OPP_LAST_{window_ngames}_PPG",
        "SEASON_PPG_AWAY": "TEAM_PPG_AWAY",
        f"LAST_{window_ngames}_PPG_AWAY": f"TEAM_LAST_{window_ngames}_PPG_AWAY",
        "SEASON_PPG_HOME": "TEAM_PPG_AT_HOME",
        f"LAST_{window_ngames}_PPG_HOME": f"TEAM_LAST_{window_ngames}_PPG_AT_HOME",
        f"LAST_{window_ngames}_PPG_STDDEV": f"ACTIVE_PLAYERS_LAST_{window_ngames}_PPG_STDDEV",
        "SEASON_PPG_STDDEV": "ACTIVE_PLAYERS_SEASON_POINTS_STDDEV",
        "HEIGHT_INCHES": "AVG_HEIGHT_INCHES",
    }

    keep_columns = [
        "gameDate",
        "homeTeam",
        "awayTeam",
        "OVER_UNDER",
        "LINE",
        f"HOME_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG",
        f"HOME_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG_STDDEV",
        f"AWAY_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG",
        f"AWAY_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG_STDDEV",
        "HOME_ACTIVE_PLAYERS_SEASON_PPG",
        "HOME_ACTIVE_PLAYERS_SEASON_POINTS_STDDEV",
        "AWAY_ACTIVE_PLAYERS_SEASON_PPG",
        "AWAY_ACTIVE_PLAYERS_SEASON_POINTS_STDDEV",
        "HOME_TEAM_OPP_PPG",
        f"HOME_TEAM_OPP_LAST_{window_ngames}_PPG",
        "AWAY_TEAM_OPP_PPG",
        f"AWAY_TEAM_OPP_LAST_{window_ngames}_PPG",
        "HOME_TEAM_PPG_AT_HOME",
        "AWAY_TEAM_PPG_AWAY",
        f"HOME_TEAM_LAST_{window_ngames}_PPG_AT_HOME",
        f"AWAY_TEAM_LAST_{window_ngames}_PPG_AWAY",
        "HOME_TEAM_2ND_OF_B2B",
        "AWAY_TEAM_2ND_OF_B2B",
        "HOME_TEAM_GAMES_PLAYED",
        "HOME_TEAM_WIN_PCT",
        "AWAY_TEAM_GAMES_PLAYED",
        "AWAY_TEAM_WIN_PCT",
        "HOME_AVG_HEIGHT_INCHES",
        "HOME_STDDEV_HEIGHT_INCHES",
        "AWAY_AVG_HEIGHT_INCHES",
        "AWAY_STDDEV_HEIGHT_INCHES",
    ]
    home_df = merged_df.loc[
        merged_df["TEAM"].isin(todays_lines["homeTeam"].to_list())
    ].copy()
    away_df = merged_df.loc[
        merged_df["TEAM"].isin(todays_lines["awayTeam"].to_list())
    ].copy()

    home_df.rename(merged_columns_enum, axis=1, inplace=True)
    home_df = home_df.add_prefix("HOME_")
    away_df.rename(merged_columns_enum, axis=1, inplace=True)
    away_df = away_df.add_prefix("AWAY_")

    todays_lines = todays_lines.merge(home_df, left_on="homeTeam", right_on="HOME_TEAM")
    todays_lines = todays_lines.merge(away_df, left_on="awayTeam", right_on="AWAY_TEAM")

    return todays_lines[keep_columns]


def fetch_new_x_data():
    print("\nGetting new X data...")
    active_player_agg_data = get_active_player_data()
    team_agg_data = sql.agg_team_new_x_data()
    todays_lines = get_todays_lines()

    merged = merge_data(team_agg_data, active_player_agg_data)

    return filter_and_align_x_data(merged, todays_lines)


def get_ou_predictions(training_data, new_x):
    print("\nTraining OU model...")
    reg_out_linear_ou, predictors = train_model(training_data, "ou")
    print("\nMaking OU predictions...")
    return reg_out_linear_ou.predict(new_x[list(predictors)])


def get_lines_predictions(training_data, new_x):
    print("\nTraining lines model...")
    reg_out_linear_lines, predictors = train_model(training_data, "lines")
    print("\nMaking lines predictions...")
    return reg_out_linear_lines.predict(new_x[list(predictors)])


def get_todays_lines():
    todays_lines_content = requests.get(
        "https://www.rotowire.com/betting/nba/tables/nba-games.php?"
    ).content
    todays_line_df = pd.DataFrame.from_records(json.loads(todays_lines_content))

    todays_line_df["homeTeam"] = todays_line_df.loc[
        todays_line_df["homeAway"] == "home", "abbr"
    ]
    todays_line_df["awayTeam"] = todays_line_df.loc[
        todays_line_df["homeAway"] == "away", "abbr"
    ]

    todays_line_df.loc[todays_line_df["homeAway"] == "away", "homeTeam"] = (
        todays_line_df.loc[todays_line_df["homeAway"] == "away", "oppAbbr"]
    )
    todays_line_df.loc[todays_line_df["homeAway"] == "home", "awayTeam"] = (
        todays_line_df.loc[todays_line_df["homeAway"] == "home", "oppAbbr"]
    )

    todays_line_df["hardrockLine"] = ""

    for n, row in todays_line_df.iterrows():
        if row["homeAway"] == "away":
            todays_line_df.at[n, "hardrockLine"] = -1 * float(row["hardrock_spread"])
        else:
            todays_line_df.at[n, "hardrockLine"] = float(row["hardrock_spread"])

    todays_line_df.rename(
        {"hardrock_ou": "OVER_UNDER", "hardrockLine": "LINE"}, axis=1, inplace=True
    )
    return todays_line_df[
        ["gameID", "gameDate", "homeTeam", "awayTeam", "OVER_UNDER", "LINE"]
    ].drop_duplicates()


def get_team_records():
    standings = ls().get_dict()["resultSets"][0]
    standings_df = pd.DataFrame(columns=standings["headers"], data=standings["rowSet"])
    standings_df["TEAM_GAMES_PLAYED"] = standings_df["WINS"] + standings_df["LOSSES"]
    standings_df.rename(
        {"WinPCT": "TEAM_WIN_PCT", "TeamID": "TEAM_ID"}, axis=1, inplace=True
    )
    return standings_df[["TEAM_ID", "TEAM_GAMES_PLAYED", "TEAM_WIN_PCT"]]


def merge_data(team_data, player_data):
    today = dt.date.today()
    yesterday = today - dt.timedelta(days=1)
    team_data["TEAM_ID"] = team_data["TEAM_ID"].astype(int)
    player_data["TEAM_ID"] = player_data["TEAM_ID"].astype(int)

    merged = team_data.merge(player_data, on="TEAM_ID")

    for x in ["LAST_GAME_DATE_HOME", "LAST_GAME_DATE_AWAY"]:
        merged[x] = pd.to_datetime(merged[x])

    merged["TEAM_2ND_OF_B2B"] = ""
    for n, row in merged.iterrows():
        if (row["LAST_GAME_DATE_HOME"] == yesterday) or (
            row["LAST_GAME_DATE_AWAY"] == yesterday
        ):
            merged.at[n, "TEAM_2ND_OF_B2B"] = 1
        else:
            merged.at[n, "TEAM_2ND_OF_B2B"] = 0

    team_records = get_team_records()

    merged = merged.merge(team_records, on="TEAM_ID")

    return merged


def fetch_predictions():
    today = dt.date.today()
    print("\nGrabbing training data...")
    training_data = sql.fetch_aggregate_betting_data()
    new_x_data = fetch_new_x_data()

    ou_predictions = get_ou_predictions(training_data, new_x_data)
    lines_predictions = get_lines_predictions(training_data, new_x_data)

    new_x_data["PREDICTED_POINT_TOTAL"] = ou_predictions
    new_x_data["PREDICTED_DIFF"] = lines_predictions

    lines_pred_df = new_x_data[["homeTeam", "awayTeam", "LINE", "PREDICTED_DIFF"]]
    ou_pred_df = new_x_data[
        ["homeTeam", "awayTeam", "OVER_UNDER", "PREDICTED_POINT_TOTAL"]
    ]

    print(f"OU Predictions: \n\n{ou_pred_df}")
    print(f"\n\nLINE Predictions: \n\n{lines_pred_df}")

    ou_pred_df.to_csv(f"ou_preds_{today}.csv", index=False)
    lines_pred_df.to_csv(f"lines_preds_{today}.csv", index=False)
