"""

INJURY REPORT SCRAPER

"""

from bs4 import BeautifulSoup
import os
import requests
import pandas as pd
from datetime import date

DOWNLOAD_LOC = os.path.join(os.environ.get("HOME"), "Desktop")
INJURY_REPORT_URL = "https://www.basketball-reference.com/friv/injuries.fcgi"


def grab_injury_report():
    page = requests.get(INJURY_REPORT_URL)
    soup = BeautifulSoup(page.content, "html.parser")

    injury_report_html = soup.find("table")

    html_headers = injury_report_html.find_all("th")

    columns = [x.text for x in html_headers]

    player_column_data = columns[4:]
    stat_headers = columns[:4]
    stat_headers.insert(3, "Status")

    nbaInjuryReportDF = pd.DataFrame(columns=stat_headers)

    injury_data = injury_report_html.find_all("tr")

    for n, row in enumerate(injury_data[1:]):
        row_data = row.find_all("td")
        individual_row_data = [
            data.text for data in row_data if data not in stat_headers
        ]
        individual_row_data.insert(0, player_column_data[n])
        status_des = individual_row_data[3].split(" - ")
        individual_row_data.insert(3, status_des[0])
        individual_row_data[4] = status_des[1]
        nbaInjuryReportDF.loc[len(nbaInjuryReportDF)] = individual_row_data

    return nbaInjuryReportDF


def export_report(injuryDF):
    today = date.today()
    injuryDF.to_csv(
        os.path.join(DOWNLOAD_LOC, f"nbaInjuryReport{today}.csv"), index=False
    )


def main():
    injuryDF = grab_injury_report()
    export_report(injuryDF)


if __name__ == "__main__":
    main()
