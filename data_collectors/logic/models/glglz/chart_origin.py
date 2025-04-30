from enum import Enum

from genie_datastores.postgres.models import Chart


class ChartOrigin(Enum):
    ISRAELI = "israeli"
    INTERNATIONAL = "international"

    def to_glglz_chart(self) -> Chart:
        origin_to_chart_mapping = {
            ChartOrigin.ISRAELI: Chart.GLGLZ_WEEKLY_ISRAELI,
            ChartOrigin.INTERNATIONAL: Chart.GLGLZ_WEEKLY_INTERNATIONAL,
        }
        return origin_to_chart_mapping[self]
