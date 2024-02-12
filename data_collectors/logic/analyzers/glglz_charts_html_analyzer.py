from typing import List, Dict, Tuple, Optional

from genie_common.tools import logger
from genie_common.utils import compute_similarity_score, extract_int_from_string, from_datetime
from genie_datastores.postgres.models import ChartEntry, Chart

from data_collectors.consts.glglz_consts import (
    ISRAELI_CHART_TITLE,
    INTERNATIONAL_CHART_TITLE,
    POSITION_TRACK_NAME_SEPARATOR,
    GLGLZ_CHART_ENTRY,
    GLGLZ_CHARTS_WEB_ELEMENT,
    CHART_NAME_SIMILARITY_THRESHOLD
)
from data_collectors.contract import IAnalyzer
from data_collectors.logic.models import GlglzChartDetails
from data_collectors.tools import WebElementsExtractor
from data_collectors.utils.glglz import generate_chart_date_url


class GlglzChartsHTMLAnalyzer(IAnalyzer):
    def __init__(self, web_elements_extractor: WebElementsExtractor = WebElementsExtractor()):
        self._web_elements_extractor = web_elements_extractor

    def analyze(self, charts_details: List[GlglzChartDetails]) -> List[ChartEntry]:
        logger.info(f"Starting to parse charts HTMLs")
        charts_entries = []

        for chart in charts_details:
            date_entries = self._generate_single_date_chart_entries(chart)
            charts_entries.extend(date_entries)

        return charts_entries

    def _generate_single_date_chart_entries(self, chart_details: GlglzChartDetails) -> List[ChartEntry]:
        logger.info(f"Parsing raw chart HTML for date `{from_datetime(chart_details.date)}`")
        elements = self._web_elements_extractor.extract(
            soup=chart_details.soup,
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
                chart_details=chart_details
            )

            if chart_entry is not None:
                charts_entries.append(chart_entry)

        return charts_entries

    def _get_relevant_chart(self, element_text: str, existing_chart: Optional[Chart]) -> Optional[Chart]:
        if self._is_chart_name_element(element_text):
            chart_name, _ = self._get_most_similar_chart_name(element_text)
            return self._names_charts_mapping[chart_name]

        return existing_chart

    def _create_single_chart_entry(self,
                                   element_text: str,
                                   chart: Chart,
                                   chart_details: GlglzChartDetails) -> Optional[ChartEntry]:
        if self._is_chart_name_element(element_text):
            return

        split_text = self._split_chart_entry_text(element_text)
        position = extract_int_from_string(split_text[0])

        if position is None:
            logger.warn(f"Was not able to convert `{split_text[0]}` to position integer. Skipping record")
            return

        entry_key = POSITION_TRACK_NAME_SEPARATOR.join(split_text[1:])
        chart_url = generate_chart_date_url(
            date=chart_details.date,
            datetime_format=chart_details.datetime_format,
            should_unquote=True
        )

        return ChartEntry(
            track_id=None,
            chart=chart,
            date=chart_details.date,
            key=entry_key,
            position=position,
            comment=chart_url
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