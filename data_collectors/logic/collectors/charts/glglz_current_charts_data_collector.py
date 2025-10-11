import re
from datetime import datetime
from typing import Optional, List, Dict

from genie_common.tools import logger
from genie_datastores.postgres.models import ChartEntry, Chart
from genie_datastores.postgres.operations import execute_query
from playwright.async_api import Browser
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.glglz_consts import (
    GLZ_CHART_TO_URL_MAP,
    WEEKLY_CHART_DATE_SUB_TITLE_ELEMENT,
    DD_MM_YYYY_DATETIME_REGEX,
    CHART_TITLE_CSS_SELECTOR,
    WEEKLY_CHART_ARTIST_ELEMENT,
    WEEKLY_CHART_SONG_ELEMENT,
    CHART_ENTRY_WEB_ELEMENT_NAME,
    GLGLZ_CHARTS_DATETIME_FORMAT,
)
from data_collectors.contract import IChartsDataCollector
from data_collectors.tools import WebElementsExtractor
from data_collectors.utils.playwright import get_page_content


class GlglzCurrentChartsDataCollector(IChartsDataCollector):
    def __init__(
        self,
        browser: Browser,
        db_engine: AsyncEngine,
        web_elements_extractor: WebElementsExtractor = WebElementsExtractor(),
    ):
        self._db_engine = db_engine
        self._browser = browser
        self._web_elements_extractor = web_elements_extractor

    async def collect(self) -> List[ChartEntry]:
        logger.info("Fetching current weekly charts pages from GLZ website")
        entries = []

        for chart, url in GLZ_CHART_TO_URL_MAP.items():
            chart_entries = await self._fetch_single_chart_entries(chart, url)
            entries.extend(chart_entries)

        return entries

    async def _fetch_single_chart_entries(self, chart: Chart, url: str) -> List[ChartEntry]:
        logger.info(f"Fetching chart `{chart.value}` entries from url `{url}`")
        html = await self._fetch_chart_page(url)
        date = self._extract_chart_date(html)
        formatted_date = date.strftime(GLGLZ_CHARTS_DATETIME_FORMAT)
        is_fresh_chart = await self._is_fresh_chart(date, chart)

        if is_fresh_chart:
            logger.info(f"Found fresh chart entries for chart `{chart.value}` on date `{formatted_date}`! Parsing")
            return self._extract_chart_entries(chart, date, html)

        logger.info(f"Chart entries for chart `{chart.value}` on date `{formatted_date}` already exist. Aborting")
        return self._extract_chart_entries(chart, date, html) if is_fresh_chart else []

    async def _fetch_chart_page(self, url: str) -> str:
        page = await self._browser.new_page()
        await page.goto(url)
        await page.wait_for_selector(CHART_TITLE_CSS_SELECTOR)

        return await get_page_content(page)

    def _extract_chart_date(self, html: str) -> Optional[datetime]:
        chart_date_element = self._web_elements_extractor.extract(html, WEEKLY_CHART_DATE_SUB_TITLE_ELEMENT)

        if chart_date_element:
            raw_chart_date = chart_date_element[0]["chart_date"]
            return self._parse_chart_date(raw_chart_date)

    @staticmethod
    def _parse_chart_date(raw_chart_date: str) -> Optional[datetime]:
        date_match = re.search(DD_MM_YYYY_DATETIME_REGEX, raw_chart_date)

        if date_match:
            return datetime.strptime(date_match.group(), GLGLZ_CHARTS_DATETIME_FORMAT)

    async def _is_fresh_chart(self, date: Optional[datetime], chart: Chart) -> bool:
        if date is None:
            logger.warning("Failed extracting or parsing chart date. Treating the charts entries as existing")
            return False

        has_existing_charts_entries = await self._has_chart_entries_on(date, chart)
        return False if has_existing_charts_entries else True

    async def _has_chart_entries_on(self, date: datetime, chart: Chart) -> bool:
        formatted_date = date.strftime(GLGLZ_CHARTS_DATETIME_FORMAT)
        logger.info(f"Querying DB to find whether any entry for chart `{chart.value}` on date `{formatted_date}` exist")
        query = select(ChartEntry.date).where(ChartEntry.chart == chart).where(ChartEntry.date == date).limit(1)
        query_result = await execute_query(engine=self._db_engine, query=query)
        existing_date = query_result.scalars().first()

        return True if existing_date else False

    def _extract_chart_entries(self, chart: Chart, date: datetime, html: str) -> List[ChartEntry]:
        artists = self._web_elements_extractor.extract(html, WEEKLY_CHART_ARTIST_ELEMENT)
        songs = self._web_elements_extractor.extract(html, WEEKLY_CHART_SONG_ELEMENT)
        entries = []

        for i, (artist, song) in enumerate(zip(artists, songs, strict=True)):
            entry = self._to_entry(
                index=i,
                artist=artist,
                song=song,
                chart=chart,
                date=date,
            )
            entries.append(entry)

        return entries

    @staticmethod
    def _to_entry(index: int, artist: Dict[str, str], song: Dict[str, str], chart: Chart, date: datetime) -> ChartEntry:
        position = index + 1
        artist_name = artist[f"{CHART_ENTRY_WEB_ELEMENT_NAME}{position}"]
        song_name = song[f"{CHART_ENTRY_WEB_ELEMENT_NAME}{position}"]

        return ChartEntry(
            chart=chart,
            date=date,
            position=position,
            key=f"{artist_name} - {song_name}",
        )
