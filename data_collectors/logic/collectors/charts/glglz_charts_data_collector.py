from time import sleep
from typing import List, Dict, Optional, Tuple
from urllib.parse import unquote

from bs4 import BeautifulSoup
from genie_common.tools import logger
from genie_common.utils import extract_int_from_string, compute_similarity_score
from genie_datastores.postgres.models import ChartEntry, Chart
from selenium.webdriver.chrome.webdriver import WebDriver

from data_collectors.consts.glglz_consts import *
from data_collectors.contract import IChartsDataCollector
from data_collectors.tools import WebElementsExtractor
from data_collectors.utils.selenium import driver_session

CHART_NAME_SIMILARITY_THRESHOLD = 0.9


class GlglzChartsDataCollector(IChartsDataCollector):
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

    def _collect_single_date_html(self, driver: WebDriver, date: datetime) -> BeautifulSoup:
        url = self._generate_chart_date_url(date)
        driver.get(url)
        sleep(5)

        return BeautifulSoup(driver.page_source, 'html.parser')

    @staticmethod
    def _generate_chart_date_url(date: datetime, should_unquote: bool = False) -> str:
        datetime_format = GLGLZ_LEGACY_DATETIME_FORMAT if date <= GLGLZ_LEGACY_END_DATE else GLGLZ_DATETIME_FORMAT
        formatted_date = date.strftime(datetime_format).strip()
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
        if self._is_chart_name_element(element_text):
            chart_name, _ = self._get_most_similar_chart_name(element_text)
            return self._names_charts_mapping[chart_name]

        return existing_chart

    def _create_single_chart_entry(self, element_text: str, chart: Chart, date: datetime) -> Optional[ChartEntry]:
        if self._is_chart_name_element(element_text):
            return

        split_text = self._split_chart_entry_text(element_text)
        position = extract_int_from_string(split_text[0])

        if position is None:
            logger.warn(f"Was not able to convert `{split_text[0]}` to position integer. Skipping record")
            return

        entry_key = POSITION_TRACK_NAME_SEPARATOR.join(split_text[1:])
        return ChartEntry(
            track_id=None,
            chart=chart,
            date=date,
            key=entry_key,
            position=position,
            comment=self._generate_chart_date_url(date, should_unquote=True)
        )

    def _is_chart_name_element(self, text: str) -> bool:
        if text.strip() == "":
            return True

        _, similarity = self._get_most_similar_chart_name(text)
        return similarity > CHART_NAME_SIMILARITY_THRESHOLD

    def _get_most_similar_chart_name(self, text: str) -> Tuple[str, float]:
        similarities = {chart: compute_similarity_score(text, chart) for chart in self._names_charts_mapping.keys()}
        most_similar_chart = max(similarities, key=lambda k: similarities[k])
        similarity = similarities[most_similar_chart]

        return most_similar_chart, similarity

    @staticmethod
    def _split_chart_entry_text(text: str) -> List[str]:
        split_text = []

        for element in text.split(POSITION_TRACK_NAME_SEPARATOR):
            stripped_element = element.strip()

            if stripped_element != "":
                split_text.append(stripped_element)

        return sorted(split_text)

    @property
    def _names_charts_mapping(self) -> Dict[str, Chart]:
        return {
            ISRAELI_CHART_TITLE: Chart.GLGLZ_WEEKLY_ISRAELI,
            INTERNATIONAL_CHART_TITLE: Chart.GLGLZ_WEEKLY_INTERNATIONAL
        }
