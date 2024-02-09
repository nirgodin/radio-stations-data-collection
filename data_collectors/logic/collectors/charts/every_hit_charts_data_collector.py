from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
from aiohttp import ClientSession
from genie_common.tools import AioPoolExecutor, logger
from genie_common.utils import chain_lists, from_datetime
from genie_datastores.postgres.models import ChartEntry, Chart
from pandas import DataFrame

from data_collectors.consts.charts_consts import CHART_KEY_FORMAT
from data_collectors.consts.every_hit_consts import EVERY_HIT_RETRO_CHARTS_BASE_URL, EVERY_HIT_POSITION_COLUMN, \
    EVERY_HIT_ARTIST_NAME_COLUMN, EVERY_HIT_TRACK_NAME_COLUMN, EVERY_HIT_ORDERED_COLUMNS
from data_collectors.contract import IChartsDataCollector
from data_collectors.logic.models import DateRange


class EveryHitChartsDataCollector(IChartsDataCollector):
    def __init__(self, session: ClientSession, pool_executor: AioPoolExecutor):
        self._session = session
        self._pool_executor = pool_executor

    async def collect(self, date_ranges: List[DateRange]) -> List[ChartEntry]:
        logger.info(f"Starting to collect charts entries for {len(date_ranges)} date ranges")
        nested_charts_entries: List[List[ChartEntry]] = await self._pool_executor.run(
            iterable=date_ranges,
            func=self._collect_single_date_range_chart,
            expected_type=list
        )
        return chain_lists(nested_charts_entries)

    async def _collect_single_date_range_chart(self, date_range: DateRange) -> Optional[List[ChartEntry]]:
        chart_data = await self._fetch_chart_data(date_range)

        if chart_data is not None:
            return self._to_chart_entries(date_range.start_date, chart_data)

    async def _fetch_chart_data(self, date_range: DateRange) -> Optional[DataFrame]:
        params = self._build_request_params(date_range)

        async with self._session.get(url=EVERY_HIT_RETRO_CHARTS_BASE_URL, params=params) as raw_response:
            raw_response.raise_for_status()
            page_content = await raw_response.text()

        return self._convert_page_content_to_dataframe(date_range, page_content)

    @staticmethod
    def _convert_page_content_to_dataframe(date_range: DateRange, page_content: str) -> Optional[DataFrame]:
        page_tables: List[DataFrame] = pd.read_html(page_content)

        if page_tables:
            return page_tables[-1].dropna(axis=1, how="all")
        else:
            start_date = from_datetime(date_range.start_date)
            end_date = from_datetime(date_range.end_date)
            logger.warn(f"Was not able to extract charts data from page between {start_date} to {end_date}")

    @staticmethod
    def _build_request_params(date_range: DateRange) -> Dict[str, str]:
        return {
            "page": "rchart",
            "sent": 1,
            "y1": date_range.start_date.strftime("%Y"),
            "m1": date_range.start_date.strftime("%m"),
            "day1": date_range.start_date.strftime("%d"),
            "y2": date_range.start_date.strftime("%Y"),
            "m2": date_range.start_date.strftime("%m"),
            "day2": date_range.start_date.strftime("%d")
        }

    def _to_chart_entries(self, start_date: datetime, data: DataFrame) -> Optional[List[ChartEntry]]:
        n_columns = len(data.columns.tolist())

        if n_columns != 3:
            logger.info(f"Number of columns did not match format. Expected 3, received {n_columns}")
            return

        return self._convert_data_to_charts_entries(start_date, data)

    @staticmethod
    def _convert_data_to_charts_entries(start_date: datetime, data: DataFrame) -> List[ChartEntry]:
        data.columns = EVERY_HIT_ORDERED_COLUMNS
        entries = []

        for _, row in data.iterrows():
            key = CHART_KEY_FORMAT.format(
                artist=row[EVERY_HIT_ARTIST_NAME_COLUMN],
                track=row[EVERY_HIT_TRACK_NAME_COLUMN]
            )
            row_entry = ChartEntry(
                chart=Chart.UK_EVERY_HIT,
                date=start_date,
                position=row[EVERY_HIT_POSITION_COLUMN],
                key=key
            )
            entries.append(row_entry)

        return entries
