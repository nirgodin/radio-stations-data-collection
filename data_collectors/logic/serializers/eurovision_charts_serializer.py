from typing import List

import pandas as pd
from pandas import DataFrame

from data_collectors.consts.eurovision_consts import EUROVISION_COUNTRY_COLUMN
from data_collectors.contract import ISerializer


class EurovisionChartsSerializer(ISerializer):
    def serialize(self, contest_results: List[DataFrame]) -> DataFrame:
        self._add_stage_column(contest_results)
        results = contest_results[-1]

        for stage_result in contest_results[:-1]:
            results = self._merge_stage_results(results, stage_result)

        return results.reset_index(drop=True)

    @staticmethod
    def _add_stage_column(contest_results: List[DataFrame]) -> None:
        for i, data in enumerate(reversed(contest_results)):
            if i == 0:
                data["stage"] = "final"
            else:
                data["stage"] = "semi_final"

    def _merge_stage_results(self, results: DataFrame, stage_results: DataFrame) -> DataFrame:
        missing_rows = self._detect_missing_countries(results, stage_results)
        missing_countries_data = stage_results[stage_results.index.isin(missing_rows)]

        return pd.concat([results, missing_countries_data])

    @staticmethod
    def _detect_missing_countries(results: DataFrame, stage_results: DataFrame) -> List[int]:
        existing_countries = results[EUROVISION_COUNTRY_COLUMN].unique().tolist()
        missing_countries_rows = []

        for i, row in stage_results.iterrows():
            country = row[EUROVISION_COUNTRY_COLUMN]

            if country not in existing_countries:
                missing_countries_rows.append(int(i))

        return missing_countries_rows
