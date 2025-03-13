import pandas as pd
import numpy as np
import json
import requests
from datetime import datetime as dt
from utility.reference import sql, injury_scraper as inj
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from xgboost import XGBRegressor
from skopt import BayesSearchCV
from skopt.space import Real, Categorical, Integer
from sklearn import tree
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import (
    train_test_split,
    cross_val_score,
    KFold,
    GridSearchCV,
)


def get_x_y(training_data: pd.DataFrame, model, window_ngames: int = 3):
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
        predictors.append("OVER_UNDER")
    elif model == "lines":
        y = "DIFF"
        predictors.append("LINE")

    return training_data[predictors], training_data[y]


def train_model(training_data, model):
    X, y = get_x_y(training_data, model)

    return LinearRegression().fit(X, y)


def get_todays_lineups():
    today = dt.today()
    year = str(today.year)
    month = today.month
    day = today.day

    if month < 10:
        month = "0" + str(month)
    if day < 10:
        month = "0" + str(day)

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
        f"https://stats.nba.com/js/data/leaders/00_daily_lineups_{year}{month}{day}.json"
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
    
    lineup_df = lineup_df.drop(columns=['lineupStatus', 'position']).drop_duplicates()
   
    return lineup_df.loc[lineup_df['rosterStatus'] == "Active"]


def cross_ref_injury_report(lineups_df):
    injury_report = inj.grab_injury_report()

    return lineups_df.loc[
        ~(
            lineups_df["playerName"].isin(
                injury_report["Player"].loc[injury_report["Status"].str.contains("Out")]
            )
        )
    ]

def fetch_new_x_data():
    lineups_df = get_todays_lineups()
    active_players = cross_ref_injury_report(lineups_df)

    active_player_agg_data = sql.agg_active_player_new_x_data(active_players)

def get_ou_predictions(training_data, new_x):
    reg_out_linear_ou = train_model(training_data, "ou")
    return reg_out_linear_ou.predict(new_x)

def get_lines_predictions(training_data, new_x):
    reg_out_linear_lines = train_model(training_data, "lines")
    return reg_out_linear_lines.predict(new_x)

def fetch_predictions():
    training_data = sql.fetch_aggregate_betting_data()
    new_x_data = fetch_new_x_data()

    ou_predictions = get_ou_predictions(training_data, new_x_data)
    lines_predictions = get_lines_predictions(training_data, new_x_data)


