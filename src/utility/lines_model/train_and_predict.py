import pandas as pd
import pandas as pd
import numpy as np
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
from sklearn.model_selection import train_test_split, cross_val_score, KFold, GridSearchCV

def get_x_y(agg_df: pd.DataFrame, model: pd.Categorical(['ou', 'lines']), window_ngames: int = 3):
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
        "AWAY_STDDEV_HEIGHT_INCHES"
    ]
    
    
    if model == 'ou':
        y = 'GAME_TOTAL_PTS'
        predictors.append('OVER_UNDER')
    elif model == 'lines':
        y = 'DIFF'
        predictors.append('LINE')
    
    return agg_df[predictors], agg_df[y]
    

def train_model(model: pd.Categorical(['ou', 'line'])):
    agg_df = sql.fetch_aggregate_betting_data()
    X, y = get_x_y(agg_df, model)

    return LinearRegression().fit(X, y)

def fetch_new_x_data():
    
    injury_df = inj.grab_injury_report()


def fetch_predictions():
    reg_out_linear_ou = train_model('ou')
    new_x_data = fetch_new_x_data()

    ou_predictions = reg_out_linear_ou.predict(new_x_data)

