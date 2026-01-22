import psycopg as ps
import pandas as pd
from sqlalchemy import create_engine, text
import getpass
from dotenv import load_dotenv
import os

load_dotenv()

USER = os.environ["sql_username"]
PASSWORD = os.environ["aws_rds_pass"]
HOST = os.environ["sql_host"]
PORT = os.environ["sql_port"]
DATABASE = os.environ["database"]


def get_connection():
    return create_engine(
        url="postgresql+psycopg://{0}:{1}@{2}:{3}/{4}".format(
            USER, PASSWORD, HOST, PORT, DATABASE
        )
    )

def execute_database_operations(statement: str):
    with get_connection().connect() as con:
        result = con.execute(text(statement))
        con.commit() 


def export_df_to_sql(
    df: pd.DataFrame,
    table_name: str | None = None,
    schema: str | None = None,
    behavior: str | None = None,
) -> None:

    if table_name == None:
        table_name = input("\nEnter table name: ")

    if schema == None:
        schema = input("\nEnter schema to import into: ")

    if behavior not in ["replace", "append", "fail"]:
        valid = False
        while valid == False:
            behavior = input("\nReplace or append? (r/a)")
            if behavior.upper() == "R":
                behavior = "replace"
                valid = True
            elif behavior.upper() == "A":
                behavior = "append"
                valid = True
            else:
                print("\nEnter a valid option.")

    df.to_sql(
        name=table_name,
        con=get_connection(),
        schema=schema,
        if_exists=behavior,
        index=False,
    )

    print(f"\n{table_name} successfully imported into {schema}.")


def convert_sql_to_df(
    table_name: str | None = None, schema: str | None = None, query: bool = False
):
    if (table_name == None) and (query == False):
        table_name = input("\nEnter table name: ")

    if (schema == None) and (query == False):
        schema = input("\nEnter schema where table is present: ")

    if query == False:
        return pd.read_sql_table(
            table_name=table_name, con=get_connection(), schema=schema
        )
    else:
        return pd.read_sql(sql=query, con=get_connection())


def fetch_aggregate_betting_data(window_ngames: int = 3, training: bool = True):
    window_ngames = str(window_ngames)
    query = f"""
   WITH lines_formatted AS (
	SELECT
		to_date("game_date", 'YYYY-MM-DD') AS "GAME_DATE",
		"home_team_abbrev" AS "HOME_TEAM",
		"visit_team_abbrev" AS "AWAY_TEAM",
		"favorite" AS "FAVORITE",
		"underdog" AS "UNDERDOG",
		"total" AS "GAME_TOTAL_PTS",
		"over_hit" AS "OVER_HIT",
		"line" AS "LINE",
		"favorite_covered" AS "FAV_HIT",
		"game_over_under" AS "OVER_UNDER",
		"home_team_score" AS "HOME_SCORE",
		"visit_team_score" AS "AWAY_SCORE"
	FROM nba_general.lines
),
player_height as (
	SELECT 
		"PERSON_ID",
		((STRING_TO_ARRAY("HEIGHT", '-'))[1]::numeric * 12) 
			+ ((STRING_TO_ARRAY("HEIGHT", '-'))[2]::numeric) AS "HEIGHT_INCHES"
	FROM nba_general.players
),
pg_cleaned as (
	SELECT
		to_date(pg_cleaned."GAME_DATE", 'YYYY-MM-DD') AS "GAME_DATE",
		RIGHT(pg_cleaned."SEASON_ID", 4)::numeric "SEASON",
		pg_cleaned."Player_ID" AS "PLAYER_ID",
		pg_cleaned."player_name" AS "PLAYER_NAME",
		player_height."HEIGHT_INCHES" AS "HEIGHT_INCHES",
		pg_cleaned."Game_ID" AS "GAME_ID",
		pg_cleaned."TEAM" AS "PLAYER_TEAM",
		pg_cleaned."PTS" AS "PLAYER_PTS",
		ROUND(AVG(pg_cleaned."PTS") OVER (
			PARTITION BY RIGHT(pg_cleaned."SEASON_ID", 4)::numeric, pg_cleaned."player_name", pg_cleaned."Player_ID"
			ORDER BY pg_cleaned."GAME_DATE"
			ROWS BETWEEN {window_ngames} PRECEDING AND 1 PRECEDING
		), 4) AS "LAST_{window_ngames}_PPG",
		ROUND(STDDEV(pg_cleaned."PTS") OVER (
			PARTITION BY RIGHT(pg_cleaned."SEASON_ID", 4)::numeric, pg_cleaned."player_name", pg_cleaned."Player_ID"
			ORDER BY pg_cleaned."GAME_DATE"
			ROWS BETWEEN {window_ngames} PRECEDING AND 1 PRECEDING
		), 4) AS "LAST_{window_ngames}_PPG_STDDEV",
		ROUND(AVG(pg_cleaned."PTS") OVER(
			PARTITION BY RIGHT(pg_cleaned."SEASON_ID", 4)::numeric, pg_cleaned."player_name", pg_cleaned."Player_ID"
			ORDER BY pg_cleaned."GAME_DATE"
			ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
		), 4) AS "SEASON_PPG",
		ROUND(STDDEV(pg_cleaned."PTS") OVER(
			PARTITION BY RIGHT(pg_cleaned."SEASON_ID", 4)::numeric, pg_cleaned."player_name", pg_cleaned."Player_ID"
			ORDER BY pg_cleaned."GAME_DATE"
			ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
		), 4) AS "SEASON_POINTS_STDDEV",
		CASE WHEN pg_cleaned."WL" = 'W' THEN 1 ELSE 0 END AS "W",
		CASE WHEN pg_cleaned."WL" = 'L' THEN 1 ELSE 0 END AS "L",
		CASE WHEN pg_cleaned."HOME/AWAY" = 'HOME' THEN "TEAM" ELSE "OPPONENT" END AS "HOME_TEAM",
		CASE WHEN pg_cleaned."HOME/AWAY" = 'AWAY' THEN "TEAM" ELSE "OPPONENT" END AS "AWAY_TEAM"
	FROM nba_gamelogs.player_gamelogs as pg_cleaned
	LEFT JOIN player_height
	ON player_height."PERSON_ID" = pg_cleaned."Player_ID"
),
gamelogs_formatted as (
	SELECT
		gamelogs_formatted."SEASON",
		gamelogs_formatted."GAME_DATE",
		gamelogs_formatted."GAME_ID",
		gamelogs_formatted."PLAYER_TEAM" AS "TEAM",
		CASE
			WHEN gamelogs_formatted."PLAYER_TEAM" = gamelogs_formatted."HOME_TEAM" 
				THEN ROUND(AVG(gamelogs_formatted."HEIGHT_INCHES"), 5) ELSE 0
					END AS "HOME_AVG_HEIGHT_INCHES",
		CASE
			WHEN gamelogs_formatted."PLAYER_TEAM" = gamelogs_formatted."HOME_TEAM" 
				THEN ROUND(STDDEV(gamelogs_formatted."HEIGHT_INCHES"), 5) ELSE 0
					END AS "HOME_STDDEV_HEIGHT_INCHES",
		CASE
			WHEN gamelogs_formatted."PLAYER_TEAM" = gamelogs_formatted."AWAY_TEAM" 
				THEN ROUND(AVG(gamelogs_formatted."HEIGHT_INCHES"), 5) ELSE 0
					END AS "AWAY_AVG_HEIGHT_INCHES",
		CASE
			WHEN gamelogs_formatted."PLAYER_TEAM" = gamelogs_formatted."AWAY_TEAM" 
				THEN ROUND(STDDEV(gamelogs_formatted."HEIGHT_INCHES"), 5) ELSE 0
					END AS "AWAY_STDDEV_HEIGHT_INCHES",
		CASE
			WHEN gamelogs_formatted."PLAYER_TEAM" = gamelogs_formatted."HOME_TEAM" 
				THEN SUM(gamelogs_formatted."PLAYER_PTS") ELSE 0
					END AS "HOME_PTS",
		CASE
			WHEN gamelogs_formatted."PLAYER_TEAM" = gamelogs_formatted."AWAY_TEAM" 
				THEN SUM(gamelogs_formatted."PLAYER_PTS") ELSE 0
					END AS "AWAY_PTS",
		CASE
			WHEN gamelogs_formatted."PLAYER_TEAM" = gamelogs_formatted."HOME_TEAM" 
				THEN SUM(gamelogs_formatted."LAST_{window_ngames}_PPG") ELSE 0
					END AS "HOME_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG",
		CASE
			WHEN gamelogs_formatted."PLAYER_TEAM" = gamelogs_formatted."HOME_TEAM" 
				THEN SUM(gamelogs_formatted."LAST_{window_ngames}_PPG_STDDEV") ELSE 0
					END AS "HOME_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG_STDDEV",
		CASE
			WHEN gamelogs_formatted."PLAYER_TEAM" = gamelogs_formatted."AWAY_TEAM" 
				THEN SUM(gamelogs_formatted."LAST_{window_ngames}_PPG")ELSE 0
					END AS "AWAY_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG",
		CASE
			WHEN gamelogs_formatted."PLAYER_TEAM" = gamelogs_formatted."AWAY_TEAM" 
				THEN SUM(gamelogs_formatted."LAST_{window_ngames}_PPG_STDDEV") ELSE 0
					END AS "AWAY_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG_STDDEV",
		CASE
			WHEN gamelogs_formatted."PLAYER_TEAM" = gamelogs_formatted."HOME_TEAM" 
				THEN SUM(gamelogs_formatted."SEASON_PPG") ELSE 0
					END AS "HOME_ACTIVE_PLAYERS_SEASON_PPG",
		CASE
			WHEN gamelogs_formatted."PLAYER_TEAM" = gamelogs_formatted."HOME_TEAM" 
				THEN SUM(gamelogs_formatted."SEASON_POINTS_STDDEV") ELSE 0
					END AS "HOME_ACTIVE_PLAYERS_SEASON_POINTS_STDDEV",
		CASE
			WHEN gamelogs_formatted."PLAYER_TEAM" = gamelogs_formatted."AWAY_TEAM" 
				THEN SUM(gamelogs_formatted."SEASON_PPG") ELSE 0
					END AS "AWAY_ACTIVE_PLAYERS_SEASON_PPG",
		CASE
			WHEN gamelogs_formatted."PLAYER_TEAM" = gamelogs_formatted."AWAY_TEAM" 
				THEN SUM(gamelogs_formatted."SEASON_POINTS_STDDEV") ELSE 0
					END AS "AWAY_ACTIVE_PLAYERS_SEASON_POINTS_STDDEV",
		gamelogs_formatted."W",
		gamelogs_formatted."L",
		gamelogs_formatted."HOME_TEAM",
		gamelogs_formatted."AWAY_TEAM"
	FROM pg_cleaned as gamelogs_formatted
	GROUP BY 
		gamelogs_formatted."SEASON",
		gamelogs_formatted."GAME_DATE",
		gamelogs_formatted."GAME_ID",
		gamelogs_formatted."PLAYER_TEAM",
		gamelogs_formatted."W",
		gamelogs_formatted."L",
		gamelogs_formatted."HOME_TEAM",
		gamelogs_formatted."AWAY_TEAM"
),
active_players_cumulative as (
	SELECT
		"SEASON",
		"GAME_DATE",
		"GAME_ID",
		"TEAM",
		"HOME_AVG_HEIGHT_INCHES",
		"HOME_STDDEV_HEIGHT_INCHES",
		"AWAY_AVG_HEIGHT_INCHES",
		"AWAY_STDDEV_HEIGHT_INCHES",
		"HOME_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG",
		"HOME_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG_STDDEV",
		"AWAY_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG",
		"AWAY_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG_STDDEV",
		"HOME_ACTIVE_PLAYERS_SEASON_PPG",
		"HOME_ACTIVE_PLAYERS_SEASON_POINTS_STDDEV",
		"AWAY_ACTIVE_PLAYERS_SEASON_PPG",
		"AWAY_ACTIVE_PLAYERS_SEASON_POINTS_STDDEV",
		SUM("W") OVER (PARTITION BY "SEASON", "TEAM" ORDER BY "GAME_DATE"
			ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS "CUM_WINS",
		SUM("L") OVER (PARTITION BY "SEASON", "TEAM" ORDER BY "GAME_DATE"
			ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS "CUM_LOSSES",
		LAST_VALUE(CASE WHEN "HOME_PTS" IS NOT NULL THEN "HOME_PTS" ELSE NULL END) 
			OVER (PARTITION BY "GAME_ID" ORDER BY "HOME_PTS" 
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "HOME_PTS",
		LAST_VALUE(CASE WHEN "AWAY_PTS" IS NOT NULL THEN "AWAY_PTS" ELSE NULL END) 
			OVER (PARTITION BY "GAME_ID" ORDER BY "AWAY_PTS" 
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "AWAY_PTS",
		CASE
			WHEN (
				"GAME_DATE" - LAG("GAME_DATE") OVER (PARTITION BY "TEAM" ORDER BY "GAME_DATE")
			) = 1 THEN 1
					ELSE 0
				END AS "2ND_OF_B2B",
		"HOME_TEAM",
		"AWAY_TEAM"
	FROM gamelogs_formatted
),
b2b_opp as (
	SELECT
		*,
		(CASE WHEN "TEAM" = "HOME_TEAM" THEN (ROUND(AVG("AWAY_PTS") OVER (
			PARTITION BY "SEASON", "TEAM" ORDER BY "GAME_DATE"
				ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
				), 4)) ELSE 0 END) AS "HOME_TEAM_OPP_PPG",
		(CASE WHEN "TEAM" = "HOME_TEAM" THEN (ROUND(AVG("AWAY_PTS") OVER (
			PARTITION BY "SEASON", "TEAM" ORDER BY "GAME_DATE"
				ROWS BETWEEN {window_ngames} PRECEDING AND 1 PRECEDING
				), 4)) ELSE 0 END) AS "HOME_TEAM_OPP_LAST_{window_ngames}_PPG",
		(CASE WHEN "TEAM" = "AWAY_TEAM" THEN (ROUND(AVG("HOME_PTS") OVER (
			PARTITION BY "SEASON", "TEAM" ORDER BY "GAME_DATE"
				ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
				), 4)) ELSE 0 END) AS "AWAY_TEAM_OPP_PPG",
		(CASE WHEN "TEAM" = "AWAY_TEAM" THEN (ROUND(AVG("HOME_PTS") OVER (
			PARTITION BY "SEASON", "TEAM" ORDER BY "GAME_DATE"
				ROWS BETWEEN {window_ngames} PRECEDING AND 1 PRECEDING
				), 4)) ELSE 0 END) AS "AWAY_TEAM_OPP_LAST_{window_ngames}_PPG",
		(CASE WHEN "TEAM" = "HOME_TEAM" THEN "2ND_OF_B2B" ELSE 0 END)
			AS "HOME_TEAM_2ND_OF_B2B",
		(CASE WHEN "TEAM" = "AWAY_TEAM" THEN "2ND_OF_B2B" ELSE 0 END)
			AS "AWAY_TEAM_2ND_OF_B2B"
	FROM active_players_cumulative
),
pergame as (
	SELECT
		"SEASON",
		"GAME_DATE",
		"GAME_ID",
		"HOME_TEAM",
		"AWAY_TEAM",
		SUM("HOME_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG") AS "HOME_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG",
		SUM("HOME_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG_STDDEV") AS "HOME_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG_STDDEV",
		SUM("AWAY_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG") AS "AWAY_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG",
		SUM("AWAY_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG_STDDEV") AS "AWAY_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG_STDDEV",
		SUM("HOME_ACTIVE_PLAYERS_SEASON_PPG") AS "HOME_ACTIVE_PLAYERS_SEASON_PPG",
		SUM("HOME_ACTIVE_PLAYERS_SEASON_POINTS_STDDEV") AS "HOME_ACTIVE_PLAYERS_SEASON_POINTS_STDDEV",
		SUM("AWAY_ACTIVE_PLAYERS_SEASON_PPG") AS "AWAY_ACTIVE_PLAYERS_SEASON_PPG",
		SUM("AWAY_ACTIVE_PLAYERS_SEASON_POINTS_STDDEV") AS "AWAY_ACTIVE_PLAYERS_SEASON_POINTS_STDDEV",
		SUM("HOME_AVG_HEIGHT_INCHES") AS "HOME_AVG_HEIGHT_INCHES",
		SUM("HOME_STDDEV_HEIGHT_INCHES") AS "HOME_STDDEV_HEIGHT_INCHES",
		SUM("AWAY_AVG_HEIGHT_INCHES") AS "AWAY_AVG_HEIGHT_INCHES",
		SUM("AWAY_STDDEV_HEIGHT_INCHES") AS "AWAY_STDDEV_HEIGHT_INCHES",
		SUM("HOME_TEAM_OPP_PPG") AS "HOME_TEAM_OPP_PPG",
		SUM("HOME_TEAM_OPP_LAST_{window_ngames}_PPG") AS "HOME_TEAM_OPP_LAST_{window_ngames}_PPG",
		SUM("AWAY_TEAM_OPP_PPG") AS "AWAY_TEAM_OPP_PPG",
		SUM("AWAY_TEAM_OPP_LAST_{window_ngames}_PPG") AS "AWAY_TEAM_OPP_LAST_{window_ngames}_PPG",
		(ROUND(AVG("HOME_PTS") OVER (
			PARTITION BY "SEASON", "HOME_TEAM" ORDER BY "GAME_DATE"
				ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
				), 4)) AS "HOME_TEAM_PPG_AT_HOME",
		(ROUND(AVG("AWAY_PTS") OVER (
			PARTITION BY "SEASON", "AWAY_TEAM" ORDER BY "GAME_DATE"
				ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
				), 4)) AS "AWAY_TEAM_PPG_AWAY",
		(ROUND(AVG("HOME_PTS") OVER (
			PARTITION BY "SEASON", "HOME_TEAM" ORDER BY "GAME_DATE"
				ROWS BETWEEN {window_ngames} PRECEDING AND 1 PRECEDING
				), 4)) AS "HOME_TEAM_LAST_{window_ngames}_PPG_AT_HOME",
		(ROUND(AVG("AWAY_PTS") OVER (
			PARTITION BY "SEASON", "AWAY_TEAM" ORDER BY "GAME_DATE"
				ROWS BETWEEN {window_ngames} PRECEDING AND 1 PRECEDING
				), 4)) AS "AWAY_TEAM_LAST_{window_ngames}_PPG_AWAY",
		SUM("HOME_TEAM_2ND_OF_B2B") AS "HOME_TEAM_2ND_OF_B2B",
		SUM("AWAY_TEAM_2ND_OF_B2B") AS "AWAY_TEAM_2ND_OF_B2B",
		MAX(CASE WHEN "TEAM" = "HOME_TEAM" THEN "CUM_WINS" ELSE 0 END) AS "HOME_TEAM_CUM_WINS",
		MAX(CASE WHEN "TEAM" = "HOME_TEAM" THEN "CUM_LOSSES" ELSE 0 END) AS "HOME_TEAM_CUM_LOSSES",
		MAX(CASE WHEN "TEAM" = "AWAY_TEAM" THEN "CUM_WINS" ELSE 0 END) AS "AWAY_TEAM_CUM_WINS",
		MAX(CASE WHEN "TEAM" = "AWAY_TEAM" THEN "CUM_LOSSES" ELSE 0 END) AS "AWAY_TEAM_CUM_LOSSES"
	FROM b2b_opp
	GROUP BY 
		"SEASON",
		"GAME_DATE",
		"GAME_ID",
		"HOME_TEAM",
		"AWAY_TEAM",
		b2b_opp."HOME_PTS",
		b2b_opp."AWAY_PTS"
),
logs_agg as (
	SELECT 
		"SEASON",
		logs_agg."GAME_DATE",
		logs_agg."HOME_TEAM",
		logs_agg."AWAY_TEAM",
		"FAVORITE",
		"HOME_SCORE",
		"AWAY_SCORE",
	    "FAV_HIT",
		("HOME_SCORE" - "AWAY_SCORE") AS "DIFF",
		"GAME_TOTAL_PTS",
	    "OVER_HIT"::numeric,
		"HOME_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG",
		"HOME_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG_STDDEV",
		"AWAY_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG",
		"AWAY_ACTIVE_PLAYERS_LAST_{window_ngames}_PPG_STDDEV",
		"HOME_ACTIVE_PLAYERS_SEASON_PPG",
		"HOME_ACTIVE_PLAYERS_SEASON_POINTS_STDDEV",
		"AWAY_ACTIVE_PLAYERS_SEASON_PPG",
		"AWAY_ACTIVE_PLAYERS_SEASON_POINTS_STDDEV",
		"HOME_TEAM_OPP_PPG",
		"HOME_TEAM_OPP_LAST_{window_ngames}_PPG",
		"AWAY_TEAM_OPP_PPG",
		"AWAY_TEAM_OPP_LAST_{window_ngames}_PPG",
		"HOME_TEAM_PPG_AT_HOME",
		"AWAY_TEAM_PPG_AWAY",
		"HOME_TEAM_LAST_{window_ngames}_PPG_AT_HOME",
		"AWAY_TEAM_LAST_{window_ngames}_PPG_AWAY",
		"HOME_TEAM_2ND_OF_B2B",
		"AWAY_TEAM_2ND_OF_B2B",
		"HOME_AVG_HEIGHT_INCHES",
		"HOME_STDDEV_HEIGHT_INCHES",
		"AWAY_AVG_HEIGHT_INCHES",
		"AWAY_STDDEV_HEIGHT_INCHES",
		("HOME_TEAM_CUM_WINS" + "HOME_TEAM_CUM_LOSSES") AS "HOME_TEAM_GAMES_PLAYED",
		(CASE WHEN ("HOME_TEAM_CUM_WINS" + "HOME_TEAM_CUM_LOSSES") != 0 THEN
			ROUND(("HOME_TEAM_CUM_WINS"::numeric / ("HOME_TEAM_CUM_WINS" + "HOME_TEAM_CUM_LOSSES")), 5)
				ELSE NULL END) AS "HOME_TEAM_WIN_PCT",
		("AWAY_TEAM_CUM_WINS" + "AWAY_TEAM_CUM_LOSSES") AS "AWAY_TEAM_GAMES_PLAYED",
		(CASE WHEN ("AWAY_TEAM_CUM_WINS" + "AWAY_TEAM_CUM_LOSSES") != 0 THEN
			ROUND(("AWAY_TEAM_CUM_WINS"::numeric / ("AWAY_TEAM_CUM_WINS" + "AWAY_TEAM_CUM_LOSSES")), 5)
				ELSE NULL END) AS "AWAY_TEAM_WIN_PCT"
	FROM pergame as logs_agg
	INNER JOIN lines_formatted
		ON lines_formatted."GAME_DATE" = logs_agg."GAME_DATE"
		AND lines_formatted."HOME_TEAM" = logs_agg."HOME_TEAM"
		AND lines_formatted."AWAY_TEAM" = logs_agg."AWAY_TEAM"
)
SELECT * FROM logs_agg;


    """

    return convert_sql_to_df(query=query)


def agg_active_player_new_x_data(active_lineup, window_ngames: int = 3):
    id_list = active_lineup["personId"].astype(str).to_list()
    query = f"""
	WITH active_players AS (
    SELECT
        pg."Player_ID",
        pg."player_name",
        pg."GAME_DATE",
        height."TEAM_ID",
        RIGHT(pg."SEASON_ID", 4)::numeric AS "SEASON_YEAR",
        ROUND(AVG(pg."PTS") OVER (
            PARTITION BY RIGHT(pg."SEASON_ID", 4)::numeric, pg."player_name", pg."Player_ID"
            ORDER BY pg."GAME_DATE"
            ROWS BETWEEN {str(window_ngames - 1)} PRECEDING AND CURRENT ROW
        ), 4) AS "LAST_{str(window_ngames)}_PPG",
        ROUND(STDDEV(pg."PTS") OVER (
            PARTITION BY RIGHT(pg."SEASON_ID", 4)::numeric, pg."player_name", pg."Player_ID"
            ORDER BY pg."GAME_DATE"
            ROWS BETWEEN {str(window_ngames - 1)} PRECEDING AND CURRENT ROW
        ), 4) AS "LAST_{str(window_ngames)}_PPG_STDDEV",
        ROUND(AVG(pg."PTS") OVER (
            PARTITION BY RIGHT(pg."SEASON_ID", 4)::numeric, pg."player_name", pg."Player_ID"
            ORDER BY pg."GAME_DATE"
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 4) AS "SEASON_PPG",
        ROUND(STDDEV(pg."PTS") OVER (
            PARTITION BY RIGHT(pg."SEASON_ID", 4)::numeric, pg."player_name", pg."Player_ID"
            ORDER BY pg."GAME_DATE"
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 4) AS "SEASON_PPG_STDDEV",
        ((STRING_TO_ARRAY(height."HEIGHT", '-'))[1]::numeric * 12) + ((STRING_TO_ARRAY(height."HEIGHT", '-'))[2]::numeric) AS "HEIGHT_INCHES",
        ROW_NUMBER() OVER (PARTITION BY pg."Player_ID" ORDER BY pg."GAME_DATE" DESC) AS "rn"
    FROM nba_gamelogs.player_gamelogs AS pg
    LEFT JOIN nba_general.players AS height ON height."PERSON_ID" = pg."Player_ID"
    WHERE pg."Player_ID" IN ({", ".join(id_list)}))
	SELECT *
	FROM active_players
	WHERE rn = 1
	ORDER BY "Player_ID", "player_name" DESC LIMIT {len(id_list)};
	"""
    return convert_sql_to_df(query=query)


def agg_team_new_x_data(window_ngames: int = 3):
    for n in ["HOME", "AWAY", "TOTAL"]:

        query = f"""
		WITH team_metrics as ( 
		SELECT 
        "GAME_DATE",
        "TEAM_ID",
		"TEAM", 
		ROUND(AVG("PTS"::numeric) OVER (PARTITION BY "SEASON_YEAR", "TEAM" 
			ORDER BY "GAME_DATE" ROWS BETWEEN 
			UNBOUNDED PRECEDING AND CURRENT ROW)::numeric, 4) AS "SEASON_PPG", 
		ROUND(AVG("PTS"::numeric) OVER (PARTITION BY "SEASON_YEAR", "TEAM" 
			ORDER BY "GAME_DATE" ROWS BETWEEN 
			{window_ngames - 1} PRECEDING AND CURRENT ROW)::numeric, 4) AS "LAST_{str(window_ngames)}_PPG",
        ROUND(AVG("PTS"::numeric - "PLUS_MINUS"::numeric) OVER (PARTITION BY "SEASON_YEAR", "TEAM"
			ORDER BY "GAME_DATE" ROWS BETWEEN
			UNBOUNDED PRECEDING AND CURRENT ROW)::numeric, 4) AS "SEASON_OPP_PPG",
		ROUND(AVG("PTS"::numeric - "PLUS_MINUS"::numeric) OVER (PARTITION BY "SEASON_YEAR", "TEAM"
			ORDER BY "GAME_DATE" ROWS BETWEEN
			{window_ngames - 1} PRECEDING AND CURRENT ROW)::numeric, 4) AS "LAST_{window_ngames}_OPP_PPG", 
        ROW_NUMBER() OVER (PARTITION BY "TEAM_ID" ORDER BY "GAME_DATE" DESC) AS "rn"
		FROM  
			nba_gamelogs.team_gamelogs 
        WHERE "HOME/AWAY" = '{n}'
		ORDER BY "GAME_DATE" DESC) 
		SELECT  
			MAX("GAME_DATE") AS "LAST_GAME_DATE",
            "TEAM_ID",
			"TEAM", 
			"SEASON_PPG", 
			"LAST_{window_ngames}_PPG", 
			"SEASON_OPP_PPG", 
			"LAST_{window_ngames}_OPP_PPG" 
		FROM team_metrics 
		WHERE rn = 1 
        GROUP BY 
        	"TEAM_ID", 
        	"TEAM", 
            "SEASON_PPG",
            "LAST_{window_ngames}_PPG", 
			"SEASON_OPP_PPG", 
			"LAST_{window_ngames}_OPP_PPG" 
		LIMIT 30; 
	"""
        if n == "HOME":
            home_df = convert_sql_to_df(query=query)
        elif n == "AWAY":
            away_df = convert_sql_to_df(query=query)
        else:
            query = query.replace(f"WHERE \"HOME/AWAY\" = '{n}'", "")
            total_df = convert_sql_to_df(query=query)
            total_df = total_df[["TEAM_ID", "SEASON_OPP_PPG", f"LAST_{window_ngames}_OPP_PPG"]]

    merged = (
        home_df.merge(away_df, on="TEAM", suffixes=("_HOME", "_AWAY"))
        .rename({"TEAM_ID_HOME": "TEAM_ID"}, axis=1)
        .drop("TEAM_ID_AWAY", axis=1)
    )
    return merged.merge(total_df, on="TEAM_ID")
