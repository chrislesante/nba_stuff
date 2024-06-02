"""

INJURY REPORT SCRAPER

"""

from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import date

def create_df():
    url = 'https://www.basketball-reference.com/friv/injuries.fcgi'

    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    
    injury_report_html = soup.find('table')
    
    html_headers = injury_report_html.find_all('th')
    
    columns = []
    for line in html_headers:
        columns.append(line.text)
    
    player_column_data = columns[4:]
    columns = columns[:4]
    
    stat_headers = columns
    
    stat_headers.insert(3,'Status')
    
    nbaInjuryReportDF = pd.DataFrame(columns=stat_headers)
    
    injury_data = injury_report_html.find_all('tr')

    idx = 0
    for row in injury_data[1:]:
        row_data = row.find_all('td')
        individual_row_data = [data.text for data in row_data if data not in stat_headers]
        individual_row_data.insert(0, player_column_data[idx])
        status_des = individual_row_data[3].split(' - ')
        individual_row_data.insert(3,status_des[0])
        individual_row_data[4] = status_des[1]
        idx += 1
        length = len(nbaInjuryReportDF)
        nbaInjuryReportDF.loc[length] = individual_row_data
        
    return nbaInjuryReportDF

def export_report(injuryDF):
    today = date.today()
    injuryDF.to_csv(f'nbaInjuryReport{today}.csv',index=False)

def main():
    injuryDF = create_df()
    export_report(injuryDF)
    
if __name__ == "__main__":
    main()