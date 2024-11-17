import psycopg as ps
import pandas as pd
from sqlalchemy import create_engine
import getpass
import os

FLATFILE_PATH = f"{os.environ['HOME']}/Desktop/nba_flatfiles/"
USER = "postgres"
PASSWORD = getpass.getpass("\nEnter SQL password: ")
HOST = "localhost"
PORT = 5433
DATABASE = "NBA"


def get_connection():
    return create_engine(
        url="postgresql+psycopg://{0}:{1}@{2}:{3}/{4}".format(
            USER, PASSWORD, HOST, PORT, DATABASE
        )
    )

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

def convert_sql_to_df(table_name: str | None = None, schema: str | None = None, query: bool = False):
    if (table_name == None) and (query == True):
        table_name = input("\nEnter table name: ")

    if (schema == None) and (query == True):
        schema = input("\nEnter schema where table is present: ")

    if query == False:
        return pd.read_sql_table(table_name=table_name, con=get_connection(), schema=schema)
    else:
        return pd.read_sql(sql=query, con=get_connection())