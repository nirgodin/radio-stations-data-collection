from typing import List, Dict

from genie_common.tools import logger
from genie_common.utils import contains_any_alpha_character, contains_any_substring
from genie_datastores.postgres.models import ChartEntry, Chart

from data_collectors.consts.glglz_consts import GLGLZ_CHART_ENTRY
from data_collectors.contract import IGlglzChartsSerializer
from data_collectors.logic.models import GlglzChartDetails, HTMLElement
from data_collectors.utils.glglz import generate_chart_date_url


class GlglzChartsSpanSerializer(IGlglzChartsSerializer):
    def serialize(self, chart_details: GlglzChartDetails, elements: List[Dict[str, str]]) -> List[ChartEntry]:
        logger.info("Serializing charts entries using span serializer")
        charts_entries = []

        for element in elements:
            n_elements = len(charts_entries)

            if self._is_valid_element(element) and n_elements <= 20:
                entry = self._create_single_chart_entry(
                    chart_details=chart_details,
                    existing_elements_number=n_elements,
                    element=element,
                )

                if self._is_new_entry(entry, charts_entries):
                    charts_entries.append(entry)

        return charts_entries

    def _create_single_chart_entry(
        self,
        chart_details: GlglzChartDetails,
        existing_elements_number: int,
        element: Dict[str, str],
    ) -> ChartEntry:
        chart_url = generate_chart_date_url(
            date=chart_details.date,
            datetime_format=chart_details.datetime_format,
            should_unquote=True,
        )
        chart = self._derive_chart(existing_elements_number)
        position = self._compute_track_position(existing_elements_number)
        key = element[GLGLZ_CHART_ENTRY]

        return ChartEntry(
            chart=chart,
            date=chart_details.date,
            position=position,
            comment=chart_url,
            key=key,
        )

    @staticmethod
    def _derive_chart(existing_elements_number: int) -> Chart:
        if existing_elements_number <= 10:
            return Chart.GLGLZ_WEEKLY_ISRAELI

        if existing_elements_number >= 20:
            logger.warn(f"Found list item with index `{existing_elements_number}`, where maximum 20 where expected")

        return Chart.GLGLZ_WEEKLY_INTERNATIONAL

    @staticmethod
    def _compute_track_position(existing_elements_number: int) -> int:
        if existing_elements_number <= 10:
            return existing_elements_number

        return existing_elements_number - 10

    @property
    def element(self) -> HTMLElement:
        return HTMLElement.SPAN

    @staticmethod
    def _is_valid_element(element: Dict[str, str]) -> bool:
        content = element[GLGLZ_CHART_ENTRY]
        return contains_any_alpha_character(content) and contains_any_substring(content, ["-", "–"])

    @staticmethod
    def _is_new_entry(contender_entry: ChartEntry, existing_entries: List[ChartEntry]) -> bool:
        return not any(contender_entry.key == entry.key for entry in existing_entries)
