from dataclasses import dataclass
from datetime import datetime
from typing import List

from genie_common.utils import random_datetime, random_alphanumeric_string, random_boolean, random_enum_value
from spotipyio.testing import SpotifyMockFactory

from data_collectors.logic.models.glglz.chart_origin import ChartOrigin
from data_collectors.logic.models.glglz.glglz_chart_details import GlglzChartDetails
from data_collectors.logic.models.glglz.localized_entity import LocalizedEntity
from data_collectors.logic.models.glglz.raw_glglz_entry import RawGlglzEntry


@dataclass
class GlglzEntryResources:
    entry: RawGlglzEntry
    track_id: str
    artist_id: str
    album_id: str

    @classmethod
    def random(cls, position: int) -> "GlglzEntryResources":
        return cls(
            entry=cls._random_entry(position),
            track_id=SpotifyMockFactory.spotify_id(),
            artist_id=SpotifyMockFactory.spotify_id(),
            album_id=SpotifyMockFactory.spotify_id(),
        )

    @staticmethod
    def _random_entry(position: int) -> RawGlglzEntry:
        return RawGlglzEntry(
            raw_value=random_alphanumeric_string(min_length=10),
            artist=GlglzEntryResources._random_localized_entity(),
            track=GlglzEntryResources._random_localized_entity(),
            position=position,
            origin=random_enum_value(ChartOrigin),
        )

    @staticmethod
    def _random_localized_entity() -> LocalizedEntity:
        return LocalizedEntity(
            name=random_alphanumeric_string(min_length=10),
        )


@dataclass
class GlglzChartResources:
    date: datetime
    html: str
    entries_resources: List[GlglzEntryResources]
    chart_details: GlglzChartDetails

    @classmethod
    def random(cls) -> "GlglzChartResources":
        date = random_datetime()
        entries_resources = [GlglzEntryResources.random(position) for position in range(1, 11)]
        return cls(
            date=date,
            html=random_alphanumeric_string(min_length=10),
            entries_resources=entries_resources,
            chart_details=cls._random_chart_details(date, entries_resources),
        )

    @staticmethod
    def _random_chart_details(date: datetime, entries_resources: List[GlglzEntryResources]) -> GlglzChartDetails:
        return GlglzChartDetails(date=date, entries=[resource.entry for resource in entries_resources])
