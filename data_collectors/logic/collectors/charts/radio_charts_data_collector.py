import os
from datetime import datetime
from tempfile import TemporaryDirectory
from typing import Generator, List

import pandas as pd
from genie_common.tools import logger
from genie_common.utils import to_datetime, extract_int_from_string
from genie_datastores.google.drive import GoogleDriveClient, GoogleDriveDownloadMetadata
from genie_datastores.postgres.models import ChartEntry, Chart
from pandas import DataFrame, Series

from data_collectors.consts.charts_consts import POSITION_COLUMN_NAME, SONG_COLUMN_NAME, ARTIST_COLUMN_NAME, \
    RADIO_CHART_SHEET_NAME_DATETIME_FORMATS, CHART_RELEVANT_COLUMNS, CHART_KEY_FORMAT
from data_collectors.contract import IChartsDataCollector


class RadioChartsDataCollector(IChartsDataCollector):
    def __init__(self, drive_client: GoogleDriveClient):
        self._drive_client = drive_client

    async def collect(self, chart_drive_files: List[dict], chart: Chart) -> List[ChartEntry]:
        logger.info(f"Starting to query {len(chart_drive_files)} charts")
        charts_entries = []

        with TemporaryDirectory() as dir_path:
            for file in chart_drive_files:
                file_path = self._download_chart_data(file, dir_path)

                for chart_entry in self._generate_charts_entries(file_path, chart):
                    charts_entries.append(chart_entry)

        return charts_entries

    def _generate_charts_entries(self, chart_data_path: str, chart: Chart) -> Generator[ChartEntry, None, None]:
        logger.info("Starting to pre process chart data")

        with pd.ExcelFile(chart_data_path) as yearly_charts_data:
            for sheet_name in yearly_charts_data.sheet_names:
                weekly_chart_data = yearly_charts_data.parse(sheet_name, header=1)
                filtered_chart_data = self._filter_weekly_chart_data(weekly_chart_data)
                chart_date = self._get_chart_date(sheet_name)

                for _, row in filtered_chart_data.iterrows():
                    yield ChartEntry(
                        track_id=None,
                        chart=chart,
                        date=chart_date,
                        key=CHART_KEY_FORMAT.format(artist=row[ARTIST_COLUMN_NAME], track=row[SONG_COLUMN_NAME]),
                        position=row[POSITION_COLUMN_NAME],
                        comment=os.path.basename(chart_data_path)
                    )

    def _filter_weekly_chart_data(self, weekly_chart_data: DataFrame) -> DataFrame:
        chart_end_index = 0

        for i, row in weekly_chart_data.iterrows():
            if self._is_valid_row(row):
                chart_end_index = i
            else:
                break

        filtered_rows_data = weekly_chart_data[weekly_chart_data.index <= chart_end_index]
        filtered_data = self._filter_data_columns(filtered_rows_data)
        self._pre_process_position_column(filtered_data)

        return filtered_data

    @staticmethod
    def _get_chart_date(sheet_name: str) -> datetime:
        if "–" in sheet_name:
            sheet_name = sheet_name.split("–")[1]

        return to_datetime(sheet_name, RADIO_CHART_SHEET_NAME_DATETIME_FORMATS)

    @staticmethod
    def _is_valid_row(row: Series) -> bool:
        for column in CHART_RELEVANT_COLUMNS:
            column_value = row.get(column)

            if column_value is not None and pd.isna(column_value):
                return False

        return True

    def _download_chart_data(self, drive_file: dict, dir_path: str) -> str:
        download_metadata = GoogleDriveDownloadMetadata.from_drive_file(
            file=drive_file,
            local_dir=dir_path
        )
        self._drive_client.download(download_metadata)

        return os.path.join(dir_path, drive_file["name"])

    @staticmethod
    def _filter_data_columns(data: DataFrame) -> DataFrame:
        columns = [column for column in CHART_RELEVANT_COLUMNS if column in data.columns]

        if len(columns) != 3:
            raise ValueError("Invalid number of columns")

        filtered_data = data[columns]
        filtered_data.columns = [POSITION_COLUMN_NAME, SONG_COLUMN_NAME, ARTIST_COLUMN_NAME]

        return filtered_data

    @staticmethod
    def _pre_process_position_column(data: DataFrame) -> None:
        data[POSITION_COLUMN_NAME] = data[POSITION_COLUMN_NAME].astype(str)
        data[POSITION_COLUMN_NAME] = data[POSITION_COLUMN_NAME].apply(extract_int_from_string)
