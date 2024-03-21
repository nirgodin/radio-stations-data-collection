from typing import List, Dict

from genie_common.tools import logger
from genie_common.utils import from_datetime
from genie_datastores.postgres.models import ChartEntry

from data_collectors.consts.glglz_consts import (
    GLGLZ_CHART_ENTRY
)
from data_collectors.contract import IAnalyzer, IGlglzChartsSerializer
from data_collectors.logic.models import GlglzChartDetails, WebElement, HTMLElement
from data_collectors.logic.serializers.glglz.glglz_list_items_serializer import GlglzChartsListItemsSerializer
from data_collectors.logic.serializers.glglz.glglz_paragraph_serializer import GlglzChartsParagraphSerializer
from data_collectors.logic.serializers.glglz.glglz_span_serializer import GlglzChartsSpanSerializer
from data_collectors.tools import WebElementsExtractor


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

        for serializer in self._prioritized_serializers:
            elements = self._extract_web_elements(serializer.element, chart_details.html)

            if len(elements) > 3:
                return serializer.serialize(chart_details, elements)

        logger.info("Could not serialize charts page content using neither of the serializers provided")
        return []

    def _extract_web_elements(self, element_type: HTMLElement, html: str) -> List[Dict[str, str]]:
        web_element = self._create_charts_web_element(element_type)
        return self._web_elements_extractor.extract(
            html=html,
            web_element=web_element
        )

    @staticmethod
    def _create_charts_web_element(element_type: HTMLElement) -> WebElement:
        return WebElement(
            name="glglz_charts_container",
            type=HTMLElement.NG_COMPONENT,
            class_="ng-star-inserted",
            child_element=WebElement(
                name=GLGLZ_CHART_ENTRY,
                type=element_type,
                multiple=True,
                enumerate=False,
                split_breaks=True
            ),
            multiple=False,
            enumerate=False
        )

    @property
    def _prioritized_serializers(self) -> List[IGlglzChartsSerializer]:
        return [
            GlglzChartsListItemsSerializer(),
            GlglzChartsSpanSerializer(),
            GlglzChartsParagraphSerializer(),
        ]
