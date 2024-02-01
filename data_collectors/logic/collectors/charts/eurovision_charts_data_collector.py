from datetime import datetime
from typing import List, Tuple, Optional

import pandas as pd
from aiohttp import ClientSession
from genie_common.tools import AioPoolExecutor, logger
from genie_datastores.postgres.models import ChartEntry, Chart
from pandas import DataFrame, Series

from data_collectors.consts.radio_charts_consts import EUROVISION_WIKIPEDIA_PAGE_URL_FORMAT, EUROVISION_KEY_COLUMNS, \
    EUROVISION_TABLE_CONTEST_ID_COLUMNS, EUROVISION_PLACE_COLUMN, EUROVISION_SONG_COLUMN, EUROVISION_ARTIST_COLUMN
from data_collectors.contract import IChartsDataCollector


class EurovisionChartsDataCollector(IChartsDataCollector):
    def __init__(self, session: ClientSession, pool_executor: AioPoolExecutor):
        self._session = session
        self._pool_executor = pool_executor

    async def collect(self, years: List[int]) -> List[ChartEntry]:
        logger.info(f"Starting to collect eurovision data for {len(years)} years")
        years_to_wiki_pages = await self._query_eurovision_wikipedia_pages(years)

        return self._to_chart_entries(years_to_wiki_pages)

    async def _query_eurovision_wikipedia_pages(self, years: List[int]) -> List[Tuple[int, str]]:
        logger.info(f"Starting to collect eurovision data for {len(years)} years")
        return await self._pool_executor.run(
            iterable=years,
            func=self._query_single_year_wikipedia_page,
            expected_type=tuple
        )

    async def _query_single_year_wikipedia_page(self, year: int) -> Tuple[int, str]:
        url = EUROVISION_WIKIPEDIA_PAGE_URL_FORMAT.format(year=year)

        async with self._session.get(url) as raw_response:
            raw_response.raise_for_status()
            response = await raw_response.text()

        return year, response

    def _to_chart_entries(self, years_to_wiki_pages: List[Tuple[int, str]]) -> List[ChartEntry]:
        charts_entries = []

        for year, page in years_to_wiki_pages:
            year_entries = self._get_single_year_char_entries(year, page)

            if year_entries is not None:
                charts_entries.extend(year_entries)

        return charts_entries

    def _get_single_year_char_entries(self, year: int, page: str) -> Optional[List[ChartEntry]]:
        logger.info(f"Converting year {year} data to charts entries")
        data = self._extract_contest_results_data(page)

        if data is None:
            logger.warn(f"Could not extract eurovision charts data entries from wikipedia. Skipping year {year} data")
        else:
            return self._covert_data_to_chart_entries(data, year)

    @staticmethod
    def _extract_contest_results_data(page: str) -> Optional[DataFrame]:
        page_tables = pd.read_html(page)

        for table in page_tables:
            if all(col in table.columns for col in EUROVISION_TABLE_CONTEST_ID_COLUMNS):
                return table

    def _covert_data_to_chart_entries(self, data: DataFrame, year: int) -> List[ChartEntry]:
        chart_entries = []

        for i, row in data.iterrows():
            entry = ChartEntry(
                chart=Chart.EUROVISION,
                date=datetime(year, 1, 1),
                position=row[EUROVISION_PLACE_COLUMN],
                key=self._build_chart_key(row),
                entry_metadata={key: row[key] for key in row.index.tolist() if key not in EUROVISION_KEY_COLUMNS}
            )
            chart_entries.append(entry)

        return chart_entries

    @staticmethod
    def _build_chart_key(row: Series) -> str:
        song = row[EUROVISION_SONG_COLUMN].strip('"')
        artist = row[EUROVISION_ARTIST_COLUMN]

        return f"{artist} - {song}"
