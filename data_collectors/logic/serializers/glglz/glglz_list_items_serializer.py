from typing import List, Dict

from genie_common.tools import logger
from genie_datastores.postgres.models import ChartEntry, Chart

from data_collectors.consts.glglz_consts import GLGLZ_CHART_ENTRY
from data_collectors.contract import IGlglzChartsSerializer
from data_collectors.logic.models import GlglzChartDetails, HTMLElement
from data_collectors.utils.glglz import generate_chart_date_url


class GlglzChartsListItemsSerializer(IGlglzChartsSerializer):
    def serialize(self, chart_details: GlglzChartDetails, elements: List[Dict[str, str]]) -> List[ChartEntry]:
        logger.info("Serializing charts entries using list items serializer")
        charts_entries = []

        for i, element in enumerate(elements):
            entry = self._create_single_chart_entry(chart_details=chart_details, index=i, element=element)
            charts_entries.append(entry)

        return charts_entries

    def _create_single_chart_entry(
        self, chart_details: GlglzChartDetails, index: int, element: Dict[str, str]
    ) -> ChartEntry:
        chart_url = generate_chart_date_url(
            date=chart_details.date,
            datetime_format=chart_details.datetime_format,
            should_unquote=True,
        )
        chart = self._derive_chart(index)
        position = self._compute_track_position(index)
        key = element[GLGLZ_CHART_ENTRY]

        return ChartEntry(
            chart=chart,
            date=chart_details.date,
            position=position,
            comment=chart_url,
            key=key,
        )

    @staticmethod
    def _derive_chart(index: int) -> Chart:
        if index < 10:
            return Chart.GLGLZ_WEEKLY_ISRAELI

        if index >= 20:
            logger.warn(f"Found list item with index `{index}`, where maximum 20 where expected")

        return Chart.GLGLZ_WEEKLY_INTERNATIONAL

    @staticmethod
    def _compute_track_position(index: int) -> int:
        if index < 10:
            return index + 1

        return index - 9

    @property
    def element(self) -> HTMLElement:
        return HTMLElement.LI
