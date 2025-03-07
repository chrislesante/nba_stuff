# nba_stuff

## Welcome!

My name is Chris and I am a data engineer/analyst. This is my personal repo for the purpose of building and maintaining a local postgres database of NBA data. The majority of data is sourced directly from the NBA's API, with the exception of the injury scraper and lines scraper, which pull from basketball-reference and rotowire respectively. This repo can be used to replicate a similar database on any machine.

## Getting started

Set up a _.env_ file at the top level of the repo that contains the following variables:

`sql_username=[YOUR POSTGRES SERVER USERNAME]`
`sql_port=[THE PORT NUMBER WHERE YOUR POSTGRES SERVER IS CONNECTED]`
`sql_host=[YOUR POSTGRES SERVER HOST NAME]`

The variables above do not necessarily contain sensitive information, but I prefer to conceal my personal configurations with environment variables. With updates to the repo that require the use of arguments containing sensitive information, it is best to store them in this _.env_ file and invoke them using `os.environ[VARIABLE]`. The _.env_ file is included in the _.gitignore_ file to ensure sensitive data is not pushed to github.

## Makefile

The `Makefile` automates command line statements to simplify running scripts in the repo.

At the command line, run `make install` to automatically initiate a virtual environment and install the required packages in the _requirements.txt_ file containing all of the dependencies required to run scripts in this repo. 

Afterwards, there are many different _make targets_ that execute different operations. Most of these will set the **PYTHONPATH** inside of the virtual environment to point to the repo path, allowing for imports from the _utility_ folder.

Here is a description of the make targets:

`make run`: will prompt the user for a script name inside the repo to run. Once a script name is entered, that script will run.

`make update_logs`: runs **update_gamelogs.py**, which downloads new gamelogs to the **player_gamelogs** table.

`make plays`: runs **new_plays.py**, which downloads new play by play data to the **play_by_play** table.

`make revert_logs`: runs **revert_gamelogs.py**, which will revert the gamelogs table to a previous version of it from an inputted flatfile path.

`make lines`: runs the **lines_analyzer.py** script. This is an interactive script that pulls betting data from rotowire and automates high-level analysis as well as providing options for varying data exports.


