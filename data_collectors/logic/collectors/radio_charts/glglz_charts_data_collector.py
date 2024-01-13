from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import unquote

from bs4 import BeautifulSoup
from genie_common.tools import logger
from genie_common.utils import extract_int_from_string
from genie_datastores.postgres.models import ChartEntry, Chart
from selenium.webdriver.chrome.webdriver import WebDriver

from data_collectors.consts.glglz_consts import GLGLZ_WEEKLY_CHART_URL_FORMAT, GLGLZ_DATETIME_FORMAT, \
    ISRAELI_CHART_TITLE, INTERNATIONAL_CHART_TITLE, GLGLZ_CHARTS_WEB_ELEMENT, POSITION_TRACK_NAME_SEPARATOR, \
    GLGLZ_CHART_ENTRY
from data_collectors.contract import ICollector
from data_collectors.tools import WebElementsExtractor
from data_collectors.utils.selenium import driver_session


class GlglzChartsDataCollector(ICollector):
    def __init__(self, web_elements_extractor: WebElementsExtractor = WebElementsExtractor()):
        self._web_elements_extractor = web_elements_extractor

    async def collect(self, dates: List[datetime]) -> List[ChartEntry]:
        logger.info("Starting to run GlglzChartsDataCollector to collect raw charts entries")
        charts_entries = []
        date_html_mapping = self._collect_charts_raw_html(dates)

        for date, soup in date_html_mapping.items():
            date_entries = self._generate_single_date_chart_entries(date, soup)
            charts_entries.extend(date_entries)

        return charts_entries

    def _collect_charts_raw_html(self, dates: List[datetime]) -> Dict[datetime, BeautifulSoup]:
        date_soup_mapping = {}

        with driver_session() as driver:
            for date in dates:
                stringified_date = date.strftime("%d-%m-%Y")
                logger.info(f"Fetching raw chart HTML for date `{stringified_date}`")
                soup = self._collect_single_date_html(driver, date)
                date_soup_mapping[date] = soup

        return date_soup_mapping

    @staticmethod
    def _collect_single_date_html(driver: WebDriver, date: datetime) -> BeautifulSoup:
        formatted_date = date.strftime(GLGLZ_DATETIME_FORMAT).strip()
        url = GLGLZ_WEEKLY_CHART_URL_FORMAT.format(date=formatted_date)
        driver.get(url)

        return BeautifulSoup(driver.page_source, 'html.parser')

    @staticmethod
    def _generate_chart_date_url(date: datetime, should_unquote: bool = False) -> str:
        formatted_date = date.strftime(GLGLZ_DATETIME_FORMAT).strip()
        url = GLGLZ_WEEKLY_CHART_URL_FORMAT.format(date=formatted_date)

        return unquote(url) if should_unquote else url

    def _generate_single_date_chart_entries(self, date: datetime, soup: BeautifulSoup) -> List[ChartEntry]:
        stringified_date = date.strftime("%d-%m-%Y")
        logger.info(f"Parsing raw chart HTML for date `{stringified_date}`")
        elements = self._web_elements_extractor.extract(
            soup=soup,
            web_element=GLGLZ_CHARTS_WEB_ELEMENT
        )
        chart = None
        charts_entries = []

        for element in elements:
            element_text = element[GLGLZ_CHART_ENTRY]
            chart = self._get_relevant_chart(element_text, chart)
            chart_entry = self._create_single_chart_entry(
                element_text=element_text,
                chart=chart,
                date=date
            )

            if chart_entry is not None:
                charts_entries.append(chart_entry)

        return charts_entries

    def _get_relevant_chart(self, element_text: str, existing_chart: Optional[Chart]) -> Optional[Chart]:
        if element_text in self._names_charts_mapping.keys():
            return self._names_charts_mapping[element_text]

        return existing_chart

    def _create_single_chart_entry(self, element_text: str, chart: Chart, date: datetime) -> Optional[ChartEntry]:
        if not self._is_chart_entry_element(element_text):
            return

        split_text = element_text.split(POSITION_TRACK_NAME_SEPARATOR)
        position = extract_int_from_string(split_text[0])
        entry_key = POSITION_TRACK_NAME_SEPARATOR.join(split_text[1:])

        return ChartEntry(
            track_id=None,
            chart=chart,
            date=date,
            key=entry_key,
            position=position,
            comment=self._generate_chart_date_url(date, should_unquote=True)
        )

    def _is_chart_entry_element(self, text: str) -> bool:
        if text.strip() == "":
            return False

        return text not in self._names_charts_mapping.keys()

    @property
    def _names_charts_mapping(self) -> Dict[str, Chart]:
        return {
            ISRAELI_CHART_TITLE: Chart.GLGLZ_WEEKLY_ISRAELI,
            INTERNATIONAL_CHART_TITLE: Chart.GLGLZ_WEEKLY_INTERNATIONAL
        }
