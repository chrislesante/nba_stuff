# nba_stuff

## Welcome!

My name is Chris and I am a data engineer/analyst/scientist. This is my personal repo for the purpose of building and maintaining a AWS postgres database of NBA data to build machine learning models and visuals to understand/make predictions on player/team performance. The majority of data is sourced directly from the NBA's API, with the exception of the injury scraper and lines scraper, which pull from basketball-reference and rotowire respectively. This repo can be used to replicate a similar database on any machine or cloud server.

## About my database

My database is currently comprised of two schemas: **nba_general** and **nba_gamelogs**.

`nba_gamelogs`: contains seven tables
    - **play_by_play** : contains play by play data going back to 1996 (when the NBA began recording play by play data). The data becomes significantly more robust in the 2013-2014 season, when Second-Spectrum began tracking advanced on court data. (sourced from NBA API)
    - **player_gamelogs** : this table contains every player's individual gamelogs going back to the 1979 season (which is when the 3pt line was introduced to the NBA). If you wanted to extract all gamelogs going back to a different season, you can change the `START_SEASON` global variable in `get_all_gamelogs.py`, to the season of your choice, and run the script. (sourced from NBA API)
    - **player_advanced_metrics** : this table contains player advanced metrics (by game) going back to the 1996 season
    - **team_advanced_metrics** : this table contains team advanced metrics (by game) going back to the 1996 season
    - **player_misc_metrics** : this table contains player miscellaneous game metrics such as _paint points_, _second chance points_, _fast break points_, _points off turnovers_, etc. going back to the 1996 season
    - **team_misc_metrics** : this table contains team miscellaneous game metrics such as _paint points_, _second chance points_, _fast break points_, _points off turnovers_, etc. going back to the 1996 season
    - **team_gamelogs** : this table contains team boxscores going back to the 2013 season. This table is primarily useful for aggregating data in the betting model's prediction pipeline.

`nba_general`: contains five tables
    - **players** - contains bio/career info for all players in NBA history (sourced from NBA API)
    - **teams** - contains bio/historical info for all teams in NBA history (sourced from NBA API)
    - **team_records** - contains info on all teams regular season and playoff W/L outcomes in NBA history
    - **lines** - contains lines data (spread, over/under, game totals, etc.) going back to the 2017 season (sourced from rotowire).
    - **champions** - contains all historical NBA champions along with the year they were awarded and their opponent.

## Getting started

Set up a _.env_ file at the top level of the repo that contains the following variables:

```
sql_username=[YOUR POSTGRES SERVER USERNAME]
sql_port=[THE PORT NUMBER WHERE YOUR POSTGRES SERVER IS CONNECTED]
sql_host=[YOUR POSTGRES SERVER HOST NAME/ENDPOINT]
database=[YOUR DATABASE NAME]
aws_rds_pass=[YOUR USERNAMES PASSWORD]
```

With updates to the repo that require the use of arguments containing sensitive information, it is best to store them in this _.env_ file and invoke them using `os.environ[VARIABLE]`. The _.env_ file is included in the _.gitignore_ file to ensure sensitive data is not pushed to github.

## Makefile

The `Makefile` automates command line statements to simplify running scripts in the repo.

At the command line, run `make install` to automatically initiate a virtual environment and install the required packages in the _requirements.txt_ file containing all of the dependencies required to run scripts in this repo. 

Afterwards, there are many different _make targets_ that execute different operations. Most of these will set the **PYTHONPATH** inside of the virtual environment to point to the repo path, allowing for imports from the _utility_ folder.

Here is a description of the make targets:

- `make run`: will prompt the user for a script name inside the repo to run. Once a script name is entered, that script will run.

- `make update_logs`: runs **update_gamelogs.py**, which uploads new gamelogs to the **player_gamelogs** table.

- `make plays`: runs **new_plays.py**, which uploads new play by play data to the **play_by_play** table.

- `make revert_logs`: runs **revert_gamelogs.py**, which will revert the gamelogs table to a previous version of it from an inputted flatfile path.

- `make lines`: runs the **lines_analyzer.py** script. This is an interactive script that pulls betting data from rotowire and automates high-level analysis as well as providing options for varying data exports.


## Noteworthy

`utility/lines_model/datamodel.py` was primarily built as an exercise in object-oriented programming and is only useful for high-level analysis. `lines_analyzer.py` can be used to make over/under and spread predictions powered by the Sci-Kit Learn module. The `fetch_predictions` function can be found in `utility/lines_model/train_and_predict.py`

## /src/notebooks

This is where Machine Learning analysis is performed to explore problems and investigate strategies. See [wl_team_boxscore_feature_importance.ipynb](https://github.com/chrislesante/nba_stuff/blob/main/src/notebooks/wl_team_boxscore_feature_importance/wl_team_boxscore_feature_importance.ipynb) for an analysis on what miscelaneous/advanced metrics are predictive of winning.

## train_and_predict.py

This script aggregates data by joining the **player_gamelogs**, **lines**, and **players** tables to train a Linear regression model to predict game point totals and point differentials for the purpose of making Over/Under and spread bets. It gathers prediction data by making an api call to find the current day's active players, cross-referencing that output with the injury report found on basketball-reference, and aggregating data using both the database and additional requests to the API.

## sql.py

The `utility/reference/sql.py` script is there to make interacting with the postgres database within python scripts much simpler as well as preprocessing for machine learning models much less arduous. The **convert_sql_to_df** function pulls data from the database into a pandas dataframe while the **export_df_to_sql** function pushes data from a pandas dataframe to the database. The **fetch_aggregate_betting_data** joins and aggregates data from the _lines_, _player_gamelogs_, and _players_ tables to provide interesting test metrics and helpful evaluation fields for machine learning models.

## Visuals

[3 Pointers in the 3 Point Era](https://datawrapper.dwcdn.net/k4ecb/2/)

[League Avg 3PA vs. Champ Avg 3PA](https://datawrapper.dwcdn.net/LMIbM/2/)

[Bam Adebayo Shot Diet Evolution](https://www.datawrapper.de/_/VPKe1/)
