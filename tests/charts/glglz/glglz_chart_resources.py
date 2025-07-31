from dataclasses import dataclass
from datetime import datetime

from genie_common.utils import random_datetime, random_alphanumeric_string, random_boolean, random_enum_value

from data_collectors.logic.models.glglz.chart_origin import ChartOrigin
from data_collectors.logic.models.glglz.glglz_chart_details import GlglzChartDetails
from data_collectors.logic.models.glglz.localized_entity import LocalizedEntity
from data_collectors.logic.models.glglz.raw_glglz_entry import RawGlglzEntry


@dataclass
class GlglzChartResources:
    date: datetime
    html: str
    chart_details: GlglzChartDetails

    @classmethod
    def random(cls) -> "GlglzChartResources":
        date = random_datetime()
        return cls(
            date=date,
            html=random_alphanumeric_string(min_length=10),
            chart_details=cls._random_chart_details(date),
        )

    @staticmethod
    def _random_chart_details(date: datetime) -> GlglzChartDetails:
        return GlglzChartDetails(
            date=date, entries=[GlglzChartResources._random_entry(position) for position in range(1, 11)]
        )

    @staticmethod
    def _random_entry(position: int) -> RawGlglzEntry:
        return RawGlglzEntry(
            raw_value=random_alphanumeric_string(min_length=10),
            artist=GlglzChartResources._random_localized_entity(),
            track=GlglzChartResources._random_localized_entity(),
            position=position,
            origin=random_enum_value(ChartOrigin),
        )

    @staticmethod
    def _random_localized_entity() -> LocalizedEntity:
        return LocalizedEntity(
            name=random_alphanumeric_string(min_length=10),
            translation=random_alphanumeric_string() if random_boolean() else None,
        )
