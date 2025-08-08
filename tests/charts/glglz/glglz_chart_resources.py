from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict

from genie_common.utils import random_datetime, random_alphanumeric_string, random_enum_value
from spotipyio.testing import SpotifyMockFactory

from data_collectors.consts.spotify_consts import TRACKS, ITEMS
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

    def to_search_query(self) -> str:
        return f"{self.entry.artist.name} - {self.entry.track.name}"

    def to_search_response(self) -> Dict[str, Dict[str, List[dict]]]:
        artists = [SpotifyMockFactory.artist(id=self.artist_id, name=self.entry.artist.name)]
        return {
            TRACKS: {
                ITEMS: [
                    SpotifyMockFactory.track(
                        id=self.track_id,
                        name=self.entry.track.name,
                        artists=artists,
                        album=SpotifyMockFactory.album(id=self.album_id, artists=artists),
                    )
                ]
            }
        }

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

    def get_tracks_ids(self) -> List[str]:
        return [entry.track_id for entry in self.entries_resources]

    def get_artists_ids(self) -> List[str]:
        return [entry.artist_id for entry in self.entries_resources]

    def get_albums_ids(self) -> List[str]:
        return [entry.album_id for entry in self.entries_resources]

    @staticmethod
    def _random_chart_details(date: datetime, entries_resources: List[GlglzEntryResources]) -> GlglzChartDetails:
        return GlglzChartDetails(date=date, entries=[resource.entry for resource in entries_resources])
