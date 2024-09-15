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
        "underdog",
        "covered_as_dog_away",
        "total_times_underdog_away",
        "hit_percentage_as_dog_away",
        "covered_as_dog_home",
        "total_times_underdog_home",
        "hit_percentage_as_dog_home",
    ],
    "favorite_split": [
        "favorite",
        "covered_as_fav_away",
        "total_times_favorite_away",
        "hit_percentage_as_fav_away",
        "covered_as_fav_home",
        "total_times_favorite_home",
        "hit_percentage_as_fav_home",
    ],
}


class LinesAnalyzer:
    def __init__(self, lines_df: pd.DataFrame):
        setattr(self, "coverage_summary", self.__create_coverage_summary_table(lines_df))

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
