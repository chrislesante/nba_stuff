import pandas as pd
from utility.reference import sql
from nba_api.stats.static import teams
from utility.lines_model.train_and_predict import fetch_predictions

TEAMS = [x["abbreviation"] for x in teams.get_teams()]

TABLES = {
    "coverage_summary": [
        "team",
        "covered_as_favorite",
        "total_times_favorite",
        "fav_hit_percentage",
        "covered_as_dog",
        "total_times_underdog",
        "dog_hit_percentage",
        "overall_hit_percentage",
    ],
    "underdog_split": [
        "team",
        "covered_as_underdog_away",
        "total_times_underdog_away",
        "hit_percentage_as_underdog_away",
        "covered_as_underdog_home",
        "total_times_underdog_home",
        "hit_percentage_as_underdog_home",
    ],
    "favorite_split": [
        "team",
        "covered_as_favorite_away",
        "total_times_favorite_away",
        "hit_percentage_as_favorite_away",
        "covered_as_favorite_home",
        "total_times_favorite_home",
        "hit_percentage_as_favorite_home",
    ],
    "over_under_splits": [
        "team",
        "ppg_home",
        "opp_ppg_home",
        "combined_ppg_home",
        "ppg_away",
        "opp_ppg_away",
        "combined_ppg_away",
        "over_hit_home",
        "over_hit_home_percentage",
        "over_hit_away",
        "over_hit_away_percentage",
    ],
}

SPLIT_COLUMNS_ENUM = {
    "favorite": {
        "home": {
            "favorite": "team",
            "favorite_covered": "covered_as_favorite_home",
            "hit_percentage": "hit_percentage_as_favorite_home",
            "total_times_favorite": "total_times_favorite_home",
        },
        "away": {
            "favorite": "team",
            "favorite_covered": "covered_as_favorite_away",
            "hit_percentage": "hit_percentage_as_favorite_away",
            "total_times_favorite": "total_times_favorite_away",
        },
    },
    "underdog": {
        "home": {
            "underdog": "team",
            "underdog_covered": "covered_as_underdog_home",
            "hit_percentage": "hit_percentage_as_underdog_home",
            "total_times_underdog": "total_times_underdog_home",
        },
        "away": {
            "underdog": "team",
            "underdog_covered": "covered_as_underdog_away",
            "hit_percentage": "hit_percentage_as_underdog_away",
            "total_times_underdog": "total_times_underdog_away",
        },
    },
}


class LinesAnalyzer:
    @staticmethod
    def print_separator():
        print()
        print("*" * 100)
        print()

    @staticmethod
    def create_selection_dict(methods: dict | list) -> dict:
        selection_dict = {}
        print()
        if type(methods) == dict:
            methods = methods.keys()

        for n, method in enumerate(methods):
            choice = str(n + 1)
            selection_dict[choice] = method
            print(choice + ". " + method)

        return selection_dict

    def __init__(self, historical_lines: pd.DataFrame, todays_lines: pd.DataFrame):
        self.raw = historical_lines
        self.coverage_summary = self.__create_coverage_summary_table(self.raw)
        self.favorite_split, self.underdog_split = self.__get_coverage_splits(self.raw)
        self.over_under_splits = self.__get_over_under_splits(self.raw)
        self.todays_lines = todays_lines
        self.report_configs = {
        "coverage": {
            "dataframe": self.coverage_summary,
            "methods": ["fav_hit_percentage", "dog_hit_percentage", "overall_hit_percentage"]
        },
        "favorites": {
            "dataframe": self.favorite_split,
            "methods": ["hit_percentage_as_favorite_away", "hit_percentage_as_favorite_home"]
        },
        "underdogs": {
            "dataframe": self.underdog_split,
            "methods": ["hit_percentage_as_underdog_away", "hit_percentage_as_underdog_home"]
        },
        "over_under": {
            "dataframe": self.over_under_splits,
            "methods": ["over_hit_home_percentage", "over_hit_away_percentage"]
        }
    }

    def __aggregate_favorites_data(self, lines_df: pd.DataFrame) -> pd.DataFrame:
        favorites_by_team_df = lines_df.copy()

        total_times_favored_dict = favorites_by_team_df.value_counts(
            "favorite"
        ).to_dict()

        fav_covered_summary = (
            favorites_by_team_df.groupby(by="favorite", as_index=False)
            .sum()
            .sort_values(by="favorite_covered", ascending=False)
        )

        fav_covered_summary["total_times_favorite"] = fav_covered_summary[
            "favorite"
        ].apply(lambda x: int(total_times_favored_dict[x]))

        fav_covered_summary["hit_percentage"] = (
            (fav_covered_summary["favorite_covered"])
            / (fav_covered_summary["total_times_favorite"])
        ) * 100

        return fav_covered_summary

    def __aggregate_underdog_data(self, lines_df: pd.DataFrame) -> pd.DataFrame:
        underdogs_by_team_df = lines_df.copy()

        total_times_dog_dict = underdogs_by_team_df.value_counts("underdog").to_dict()

        dog_covered_summary = (
            underdogs_by_team_df.groupby(by="underdog", as_index=False)
            .sum()
            .sort_values(by="underdog", ascending=False)
        )

        dog_covered_summary["total_times_underdog"] = dog_covered_summary[
            "underdog"
        ].apply(lambda x: int(total_times_dog_dict[x]))

        dog_covered_summary["hit_percentage"] = (
            (dog_covered_summary["underdog_covered"])
            / (dog_covered_summary["total_times_underdog"])
        ) * 100

        return dog_covered_summary

    def __rename_fav_dog_columns(self, fav_df: pd.DataFrame, dog_df: pd.DataFrame):
        dog_df = dog_df.rename(
            {
                "underdog_covered": "covered_as_dog",
                "hit_percentage": "dog_hit_percentage",
                "underdog": "team",
            },
            axis=1,
        )

        fav_df = fav_df.rename(
            {
                "favorite_covered": "covered_as_favorite",
                "hit_percentage": "fav_hit_percentage",
                "favorite": "team",
            },
            axis=1,
        )

        return fav_df, dog_df

    def __merge_fav_dog_for_summary(self, fav_df: pd.DataFrame, dog_df: pd.DataFrame):
        coverage_summary = fav_df.merge(dog_df, how="inner", on="team")
        coverage_summary["overall_hit_percentage"] = (
            (
                coverage_summary["covered_as_favorite"]
                + coverage_summary["covered_as_dog"]
            )
            / (
                coverage_summary["total_times_favorite"]
                + coverage_summary["total_times_underdog"]
            )
            * 100
        )

        coverage_summary = coverage_summary[TABLES["coverage_summary"]]

        coverage_summary = coverage_summary.sort_values(
            by="overall_hit_percentage", ascending=False
        ).reset_index(drop=True)

        return coverage_summary

    def __create_coverage_summary_table(self, lines_df: pd.DataFrame) -> pd.DataFrame:
        fav_df = self.__aggregate_favorites_data(lines_df)

        dog_df = self.__aggregate_underdog_data(lines_df)

        fav_df, dog_df = self.__rename_fav_dog_columns(fav_df, dog_df)

        coverage_summary = self.__merge_fav_dog_for_summary(fav_df, dog_df)

        return coverage_summary

    def __get_splits(self, df: pd.DataFrame, type: str, home_away: str):
        total_times_dict = df.value_counts(type).to_dict()

        split_summary = (
            df.groupby(by=type, as_index=False)
            .sum()
            .sort_values(by=f"{type}_covered", ascending=False)
        )

        split_summary[f"total_times_{type}_{home_away}"] = split_summary[
            f"{type}"
        ].apply(lambda x: int(total_times_dict[x]))

        split_summary["hit_percentage"] = (
            (split_summary[f"{type}_covered"])
            / (split_summary[f"total_times_{type}_{home_away}"])
        ) * 100

        split_summary.sort_values(by="hit_percentage", ascending=False)

        split_summary.rename(
            SPLIT_COLUMNS_ENUM[type][home_away],
            axis=1,
            inplace=True,
        )

        return split_summary

    def __aggregate_split_data(self, lines_df: pd.DataFrame, type: str):
        at_home_df = self.get_home_data(type)
        away_df = self.get_away_data(type)

        at_home_df = self.__get_splits(at_home_df, type, "home")
        away_df = self.__get_splits(away_df, type, "away")

        split_df = at_home_df.merge(away_df, how="inner", on="team")

        split_df = split_df.sort_values(
            by=f"hit_percentage_as_{type}_away", ascending=False
        )

        split_df = split_df[TABLES[f"{type}_split"]]

        return split_df

    def __get_coverage_splits(self, lines_df: pd.DataFrame):
        fav_splits = self.__aggregate_split_data(lines_df, "favorite")
        dog_splits = self.__aggregate_split_data(lines_df, "underdog")

        return fav_splits, dog_splits

    def __aggregate_ou_split_details(self, home_visit, ou_df: pd.DataFrame):
        if home_visit == "home":
            opp = "visit"
            home_away = "home"
        else:
            home_away = "away"
            opp = "home"

        new_df = ou_df.groupby(by=f"{home_visit}_team_abbrev", as_index=False).mean(
            numeric_only=True
        )

        total_times_hit = ou_df.groupby(
            by=f"{home_visit}_team_abbrev", as_index=False
        ).sum()

        new_df = new_df.rename(
            {
                f"{home_visit}_team_abbrev": "team",
                f"{home_visit}_team_score": f"ppg_{home_away}",
                f"{opp}_team_score": f"opp_ppg_{home_away}",
                "game_over_under": f"average_ou_{home_away}",
                "over_hit": f"over_hit_{home_away}_percentage",
                "under_hit": f"under_hit_{home_away}_percentage",
            },
            axis=1,
        )

        total_times_hit = total_times_hit.rename(
            {
                f"{home_visit}_team_abbrev": "team",
                "over_hit": f"over_hit_{home_away}",
                "under_hit": f"under_hit_{home_away}",
            },
            axis=1,
        )

        merge_df = new_df.merge(
            total_times_hit, on="team", how="inner", suffixes=(None, "_y")
        )

        return merge_df

    def __get_over_under_splits(self, lines_df: pd.DataFrame) -> pd.DataFrame:
        ou_df = lines_df.copy()
        ou_df_home = self.__aggregate_ou_split_details("home", ou_df)
        ou_df_away = self.__aggregate_ou_split_details("visit", ou_df)

        ou_df_merged = ou_df_home.merge(
            ou_df_away, on="team", how="inner", suffixes=(None, "_y")
        )

        ou_df_merged["combined_ppg_home"] = (
            ou_df_merged["ppg_home"] + ou_df_merged["opp_ppg_home"]
        )
        ou_df_merged["combined_ppg_away"] = (
            ou_df_merged["ppg_away"] + ou_df_merged["opp_ppg_away"]
        )

        ou_df_merged = ou_df_merged[TABLES["over_under_splits"]]

        return ou_df_merged

    def get_new_coverage_summary(self):
        coverage_summary = pd.DataFrame(
            columns=[
                "team",
                "home_away",
                "fav_dog",
                "result",
                "average_spread",
                "std_spread",
                "total_games",
                "1_std_away",
            ]
        )
        for team in TEAMS:
            query = f"""
        WITH coverage_summary as (
                    SELECT
                        '{team}' AS "team",
                        (CASE WHEN "visit_team_abbrev" = '{team}' THEN 'away' ELSE 'home' END) AS "home_away",
                        (CASE WHEN "favorite" = '{team}' THEN 'favorite' ELSE 'underdog' END) AS "fav_dog",
                        (CASE WHEN
                            (("favorite_covered" = 1
                            AND 
                            (CASE WHEN "favorite" = '{team}' THEN 'favorite' ELSE 'underdog' END) = 'favorite') 
                        OR 
                            ("favorite_covered" = 0 
                            AND 
                            (CASE WHEN "favorite" = '{team}' THEN 'favorite' ELSE 'underdog' END) = 'underdog')) 
                                THEN 'covered' ELSE 'failed' END) AS "result",
                        ROUND(AVG("spread")::numeric, 2) as "average_spread",
                        ROUND(stddev_samp("spread")::numeric, 2) as "std_spread",
                        COUNT(*) AS "total_games"
                    FROM nba_general.lines
                    WHERE
                        ((visit_team_abbrev = '{team}')
                        OR
                        (home_team_abbrev = '{team}'))
                    AND (
                        (("favorite" = '{team}')
                        OR
                        ("underdog" = '{team}'))
                    )
                    GROUP BY "team", "home_away", "fav_dog", "result"
                    ORDER BY "home_away" DESC, "fav_dog"
                    )
        SELECT *,
        (CASE WHEN "fav_dog" = 'underdog' THEN ("average_spread" - "std_spread")
        ELSE ("average_spread" + "std_spread") END) as "1_std_away"
        FROM coverage_summary;"""

            team_summary = sql.convert_sql_to_df(
                table_name="lines", schema="nba_general", query=query
            )
            coverage_summary = pd.concat([coverage_summary, team_summary])

        self.new_coverage_report = coverage_summary
        print(self.new_coverage_report)
        self.new_coverage_report .to_csv("new_coverage_report.csv", index=False)
        print("new_coverage_report.csv exported successfully!")

    def get_home_data(self, type: str) -> pd.DataFrame:
        at_home_df = self.raw[self.raw[type] == self.raw["home_team_abbrev"]].copy()

        return at_home_df

    def get_away_data(self, type: str) -> pd.DataFrame:
        away_df = self.raw[self.raw[type] == self.raw["visit_team_abbrev"]].copy()

        return away_df

    def get_todays_lines(self):
        self.print_separator()
        if self.todays_lines.empty:
            print("No lines found for today.")
        else:
            print(self.todays_lines)
            self.todays_lines.to_csv("todays_lines.csv", index=False)
        self.print_separator()
    

    def choose_picks(self):
        self.print_separator()
        if self.todays_lines.empty:
            print("\nSorry, there are no games to predict.\n")
        else:
            fetch_predictions()
        self.print_separator()

    def get_sorted_report(self, dataframe, methods):
        self.print_separator()
        selection_dict = self.create_selection_dict(methods)
        choice = input("\nSelect an option to sort by: ")
        descending = input("\nDescending or ascending order (d/return): ")
        
        ascending = descending.upper() != "D"
        print(dataframe.sort_values(by=selection_dict[choice], ascending=ascending))
        self.print_separator()

    def get_coverage_report(self):
        methods = ["fav_hit_percentage", "dog_hit_percentage", "overall_hit_percentage"]
        self.get_sorted_report(self.coverage_summary, methods)

    def get_favorite_splits(self):
        methods = ["hit_percentage_as_favorite_away", "hit_percentage_as_favorite_home"]
        self.get_sorted_report(self.favorite_split, methods)

    def get_underdog_splits(self):
        methods = ["hit_percentage_as_underdog_away", "hit_percentage_as_underdog_home"]
        self.get_sorted_report(self.underdog_split, methods)

    def get_over_under_splits(self):
        methods = ["over_hit_home_percentage", "over_hit_away_percentage"]
        self.get_sorted_report(self.over_under_splits, methods)
    
    def export_data(self):
        self.coverage_summary.sort_values(by="team", inplace=True)
        self.coverage_summary.to_csv("coverage.csv", index=False)
        self.underdog_split.to_csv("underdog_split.csv", index=False)
        self.favorite_split.to_csv("favorite_split.csv", index=False)
        self.over_under_splits.to_csv("over_under_splits.csv", index=False)
        self.todays_lines.to_csv("todays_lines.csv", index=False)

        print("\nSummaries exported successfully!\n")
    
    def export_as_html(self):
        html_string = (
            """
                        <!DOCTYPE html>
                        <html>
                        <head>
                        <title>DataFrames</title>
                        </head>
                        <body>
                        <h1>Coverage Summary</h1>
                        """
            + self.coverage_summary.to_html()
            + """
                        <h1>Underdog Splits</h1>
                        """
            + self.underdog_split.to_html()
            + """
                        <h1>Favorite Splits</h1>
                        """
            + self.favorite_split.to_html()
            + """
                        <h1>Over Under Splits</h1>
                        """
            + self.over_under_splits.to_html()
            + """
                        <h1>Todays Lines</h1>
                        """
            + self.todays_lines.to_html()
            + """
                        </body>
                        </html>
                        """
        )

        with open("lines.html", "w") as f:
            f.write(html_string)
        
    def update_sql_table(self):
        sql.export_df_to_sql(self.raw, table_name='lines', schema='nba_general')

        