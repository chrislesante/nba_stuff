import psycopg as ps
import pandas as pd
from sqlalchemy import create_engine
 
file_path = input("\nEnter csv path to import: ")

table_name = input("\nEnter table name: ")

schema = input("\nEnter schema to import into: ")

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

user = 'postgres'
password = input("\nEnter SQL password: ")
host = 'localhost'
port = 5433
database = 'NBA'

def get_connection():
    return create_engine(
        url="postgresql+psycopg://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database
        )
    )

table = pd.read_csv(file_path)

table.to_sql(name=table_name, con=get_connection(), schema=schema, if_exists=behavior, index=False)

print("Table imported to SQL.")

