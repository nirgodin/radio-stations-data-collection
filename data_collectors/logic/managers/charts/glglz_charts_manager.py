from datetime import datetime
from typing import Optional, List, Dict, Any

from genie_common.tools import logger
from genie_datastores.postgres.models import ChartEntry, Chart
from genie_datastores.postgres.operations import execute_query
from playwright.async_api import Browser
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.glglz_consts import (
    WEEKLY_CHART_PREFIX,
    GLGLZ_CHARTS_ARCHIVE_URL,
    GLGLZ_CHARTS_LINKS_WEB_ELEMENT,
)
from data_collectors.logic.collectors import (
    GlglzChartsDataCollector,
    ChartsTracksCollector,
)
from data_collectors.logic.inserters.postgres import (
    SpotifyInsertionsManager,
    ChartEntriesDatabaseInserter,
)
from data_collectors.logic.managers.charts.base_charts_manager import BaseChartsManager
from data_collectors.tools import WebElementsExtractor
from data_collectors.utils.charts import is_valid_glglz_chart_url
from data_collectors.utils.playwright import get_page_content


class GlglzChartsManager(BaseChartsManager):
    def __init__(
        self,
        charts_data_collector: GlglzChartsDataCollector,
        charts_tracks_collector: ChartsTracksCollector,
        spotify_insertions_manager: SpotifyInsertionsManager,
        chart_entries_inserter: ChartEntriesDatabaseInserter,
        browser: Browser,
        db_engine: AsyncEngine,
    ):
        super().__init__(
            charts_data_collector=charts_data_collector,
            charts_tracks_collector=charts_tracks_collector,
            spotify_insertions_manager=spotify_insertions_manager,
            chart_entries_inserter=chart_entries_inserter,
        )
        self._browser = browser
        self._db_engine = db_engine

    async def _generate_data_collector_kwargs(
        self, dates: Optional[List[datetime]], limit: Optional[int]
    ) -> Dict[str, Any]:
        charts_urls = await self._fetch_charts_urls()
        existing_urls = await self._query_existing_dates_urls()
        urls = await self._filter_non_existing_urls(charts_urls=charts_urls, existing_urls=existing_urls, limit=limit)

        return {"urls": urls}

    async def _fetch_charts_urls(self) -> List[str]:
        logger.info("Fetching chart urls from glglz web archive")
        html = await self._fetch_charts_archive_page()
        elements = WebElementsExtractor().extract(html, GLGLZ_CHARTS_LINKS_WEB_ELEMENT)

        return self._convert_web_elements_to_urls(elements)

    async def _fetch_charts_archive_page(self) -> str:
        page = await self._browser.new_page()
        await page.goto(GLGLZ_CHARTS_ARCHIVE_URL)

        return await get_page_content(page)

    @staticmethod
    def _convert_web_elements_to_urls(elements: List[Optional[Dict[str, str]]]) -> List[str]:
        urls = []

        for element in elements:
            if element is None:
                continue

            for title, route in element.items():
                if title.__contains__(WEEKLY_CHART_PREFIX):
                    formatted_route = route.lstrip("/")
                    url = f"https://glz.co.il/{formatted_route}"
                    urls.append(url)

        return urls

    async def _query_existing_dates_urls(self) -> List[str]:
        logger.info("Querying database for existing charts urls")
        query = select(ChartEntry.comment).where(
            ChartEntry.chart.in_([Chart.GLGLZ_WEEKLY_ISRAELI, Chart.GLGLZ_WEEKLY_INTERNATIONAL])
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().all()

    async def _filter_non_existing_urls(
        self, charts_urls: List[str], existing_urls: List[str], limit: Optional[int]
    ) -> List[str]:
        if limit is None:
            limit = len(charts_urls)

        non_existing_urls = []

        for url in charts_urls:
            if url not in existing_urls:
                if await self._is_valid_url(url):
                    non_existing_urls.append(url)

            if len(non_existing_urls) >= limit:
                break

        return non_existing_urls

    async def _is_valid_url(self, url: str) -> bool:
        page = await self._browser.new_page()
        await page.goto(url)
        content = await get_page_content(page, sleep_between=2)

        return is_valid_glglz_chart_url(content, url)

    @staticmethod
    def _is_ok_response(page_source: str, url: str) -> bool:
        "is temporarily unavailable"
        if "custom 404" in page_source.lower():
            logger.info(f"Did not manage to find charts entries in url `{url}`. Skipping")
            return False

        logger.info(f"Found charts entries in url `{url}`! Parsing page source")
        return True
