import pandas as pd

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
    def __init__(self, lines_df: pd.DataFrame):
        setattr(self, "raw", lines_df)
        setattr(
            self, "coverage_summary", self.__create_coverage_summary_table(lines_df)
        )

        fav_splits, dog_splits = self.__get_coverage_splits(lines_df)

        setattr(self, "underdog_split", dog_splits)
        setattr(self, "favorite_split", fav_splits)
        setattr(self, "over_under_splits", self.__get_over_under_splits(lines_df))

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

        merge_df = new_df.merge(total_times_hit, on="team", how="inner", suffixes=(None, "_y"))

        return merge_df

    def __get_over_under_splits(self, lines_df: pd.DataFrame) -> pd.DataFrame:
        ou_df = lines_df.copy()
        ou_df_home = self.__aggregate_ou_split_details("home", ou_df)
        ou_df_away = self.__aggregate_ou_split_details("visit", ou_df)

        ou_df_merged = ou_df_home.merge(ou_df_away, on="team", how="inner", suffixes=(None, "_y"))

        ou_df_merged["combined_ppg_home"] = ou_df_merged["ppg_home"] + ou_df_merged["opp_ppg_home"]
        ou_df_merged["combined_ppg_away"] = ou_df_merged["ppg_away"] + ou_df_merged["opp_ppg_away"]

        ou_df_merged.to_csv("ou_df_merged")
        ou_df_merged = ou_df_merged[TABLES["over_under_splits"]]

        return ou_df_merged

    def get_home_data(self, type: str) -> pd.DataFrame:
        at_home_df = self.raw[self.raw[type] == self.raw["home_team_abbrev"]].copy()

        return at_home_df

    def get_away_data(self, type: str) -> pd.DataFrame:
        away_df = self.raw[self.raw[type] == self.raw["visit_team_abbrev"]].copy()

        return away_df
