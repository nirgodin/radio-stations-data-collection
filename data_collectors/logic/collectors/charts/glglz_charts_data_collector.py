from datetime import datetime
from functools import partial
from typing import List, Optional

from genie_common.tools import logger, AioPoolExecutor
from genie_common.utils import from_datetime
from genie_datastores.postgres.models import ChartEntry
from playwright.async_api import Browser, async_playwright

from data_collectors.consts.glglz_consts import GLGLZ_DATETIME_FORMATS
from data_collectors.contract import IChartsDataCollector
from data_collectors.logic.analyzers import GlglzChartsHTMLAnalyzer
from data_collectors.logic.models import GlglzChartDetails
from data_collectors.utils.glglz import generate_chart_date_url
from data_collectors.utils.playwright import get_page_content


class GlglzChartsDataCollector(IChartsDataCollector):
    def __init__(
        self,
        pool_executor: AioPoolExecutor,
        html_analyzer: GlglzChartsHTMLAnalyzer = GlglzChartsHTMLAnalyzer(),
    ):
        self._pool_executor = pool_executor
        self._html_analyzer = html_analyzer

    async def collect(self, dates: List[datetime]) -> List[ChartEntry]:
        logger.info(
            "Starting to run GlglzChartsDataCollector to collect raw charts entries"
        )

        async with async_playwright() as p:
            browser = await p.chromium.launch()
            charts_details = await self._pool_executor.run(
                iterable=dates,
                func=partial(self._collect_single_date_details, browser),
                expected_type=GlglzChartDetails,
            )

        return self._html_analyzer.analyze(charts_details)

    async def _collect_single_date_details(
        self, browser: Browser, date: datetime
    ) -> Optional[GlglzChartDetails]:
        logger.info(f"Fetching raw chart HTML for date `{from_datetime(date)}`")

        for datetime_format in GLGLZ_DATETIME_FORMATS:
            page_source = await self._fetch_page_source(date, datetime_format, browser)

            if self._is_ok_response(page_source, date, datetime_format):
                return GlglzChartDetails(
                    date=date, datetime_format=datetime_format, html=page_source
                )

        logger.warn(
            f"Did not find chart page for date `{from_datetime(date)}`. Skipping"
        )

    @staticmethod
    async def _fetch_page_source(
        date: datetime, datetime_format: str, browser: Browser
    ) -> Optional[str]:
        url = generate_chart_date_url(date, datetime_format)
        page = await browser.new_page()
        await page.goto(url)

        return await get_page_content(page)

    @staticmethod
    def _is_ok_response(page_source: str, date: datetime, datetime_format: str) -> bool:
        stringified_date = from_datetime(date)

        if "custom 404" in page_source.lower():
            logger.info(
                f"Format `{datetime_format}` is invalid for chart from date `{stringified_date}`. Skipping"
            )
            return False

        logger.info(
            f"Format `{datetime_format}` matched chart from date `{stringified_date}`! Parsing page source"
        )
        return True
