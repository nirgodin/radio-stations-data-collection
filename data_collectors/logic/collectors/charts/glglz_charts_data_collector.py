from datetime import datetime
from time import sleep
from typing import List, Optional
from urllib.parse import unquote

from bs4 import BeautifulSoup
from genie_common.tools import logger
from genie_common.utils import from_datetime
from genie_datastores.postgres.models import ChartEntry
from selenium.webdriver.chrome.webdriver import WebDriver

from data_collectors.consts.glglz_consts import GLGLZ_DATETIME_FORMATS, GLGLZ_WEEKLY_CHART_URL_FORMAT
from data_collectors.contract import IChartsDataCollector
from data_collectors.logic.analyzers import GlglzChartsHTMLAnalyzer
from data_collectors.logic.models import GlglzChartDetails
from data_collectors.utils.glglz import generate_chart_date_url
from data_collectors.utils.selenium import driver_session


class GlglzChartsDataCollector(IChartsDataCollector):
    def __init__(self, html_analyzer: GlglzChartsHTMLAnalyzer = GlglzChartsHTMLAnalyzer()):
        self._html_analyzer = html_analyzer

    async def collect(self, dates: List[datetime]) -> List[ChartEntry]:
        logger.info("Starting to run GlglzChartsDataCollector to collect raw charts entries")
        charts_details = self._collect_charts_raw_html(dates)

        return self._html_analyzer.analyze(charts_details)

    def _collect_charts_raw_html(self, dates: List[datetime]) -> List[GlglzChartDetails]:
        charts_details = []

        with driver_session() as driver:
            for date in dates:
                logger.info(f"Fetching raw chart HTML for date `{from_datetime(date)}`")
                chart_details = self._collect_single_date_details(driver, date)

                if chart_details is not None:
                    charts_details.append(chart_details)

        return charts_details

    def _collect_single_date_details(self, driver: WebDriver, date: datetime) -> Optional[GlglzChartDetails]:
        for datetime_format in GLGLZ_DATETIME_FORMATS:
            url = generate_chart_date_url(date, datetime_format)
            driver.get(url)
            sleep(3)

            if self._is_ok_response(driver.page_source, date, datetime_format):
                return GlglzChartDetails(
                    date=date,
                    datetime_format=datetime_format,
                    soup=BeautifulSoup(driver.page_source, 'html.parser')
                )

        logger.warn(f"Did not find chart page for date `{from_datetime(date)}`. Skipping")

    @staticmethod
    def _is_ok_response(page_source: str, date: datetime, datetime_format: str) -> bool:
        stringified_date = from_datetime(date)

        if "custom 404" in page_source.lower():
            logger.info(f"Format `{datetime_format}` is invalid for chart from date `{stringified_date}`. Skipping")
            return False

        logger.info(f"Format `{datetime_format}` matched chart from date `{stringified_date}`! Parsing page source")
        return True
