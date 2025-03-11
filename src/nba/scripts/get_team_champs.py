import pandas as pd
from nba_api.stats.endpoints import teamdetails as td
from nba_api.stats.static import teams
from utility.reference import sql
import time

TEAMS = teams.get_teams()
HEADERS = ['TEAM', 'YEARAWARDED', 'OPPOSITETEAM']

def fetch_team_details(team_json: dict) -> dict:
    return td.TeamDetails(team_json['id']).get_dict()['resultSets'][3]['rowSet']

def main():
    champ_df = pd.DataFrame(columns=HEADERS)
    for team in TEAMS:
        data = fetch_team_details(team)
        try:
            for n, row in enumerate(data):
                row.insert(0, team['abbreviation'])
                data[n] = row
            df = pd.DataFrame(columns=HEADERS, data=data)
            champ_df = pd.concat([champ_df, df])
        except IndexError:
            print(f"No championship data found - {team['abbreviation']}")
            continue
        time.sleep(0.6)
    
    sql.export_df_to_sql(df=champ_df, table_name='champions', schema='general', behavior='replace')

if __name__ == '__main__':
    main()