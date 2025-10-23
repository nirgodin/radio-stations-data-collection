from datetime import datetime
from typing import List, Type, Any, Optional

from genie_common.utils import safe_nested_get, to_datetime
from genie_datastores.postgres.inner_utils.spotify_utils import extract_artist_id
from genie_datastores.postgres.models import SpotifyTrack
from genie_datastores.postgres.models.orm.spotify.base_spotify_orm_model import (
    BaseSpotifyORMModel,
)

from data_collectors.consts.spotify_consts import (
    TRACKS,
    TRACK,
    ID,
    NAME,
    ALBUM,
    RELEASE_DATE,
    EXPLICIT,
    TRACK_NUMBER,
    ARTISTS,
)
from data_collectors.consts.datetime_consts import SPOTIFY_RELEASE_DATE_ORDERED_FORMATS
from data_collectors.logic.inserters.postgres import BaseIDsDatabaseInserter


class SpotifyTracksDatabaseInserter(BaseIDsDatabaseInserter):
    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        return tracks

    def _to_record(self, raw_record: dict) -> Optional[SpotifyTrack]:
        if self._is_serializable(raw_record):
            inner_track = raw_record[TRACK]

            return SpotifyTrack(
                id=inner_track[ID],
                artist_id=extract_artist_id(inner_track),
                album_id=safe_nested_get(inner_track, [ALBUM, ID], default=None),
                explicit=inner_track.get(EXPLICIT, False),
                name=inner_track[NAME],
                number=inner_track[TRACK_NUMBER],
                release_date=self._extract_release_date(inner_track),
            )

    def _is_serializable(self, raw: Any) -> bool:
        if isinstance(raw, dict):
            inner_track = raw.get(TRACK)

            if isinstance(inner_track, dict) and self._contains_required_fields(inner_track):
                return self._has_primary_artist(inner_track)

        return False

    @staticmethod
    def _contains_required_fields(inner_track: dict) -> bool:
        return all(key in inner_track.keys() for key in [ID, NAME, EXPLICIT, TRACK_NUMBER])

    @staticmethod
    def _has_primary_artist(inner_track: dict) -> bool:
        artists = inner_track.get(ARTISTS)

        if isinstance(artists, list) and len(artists) > 0:
            first_artist = artists[0]
            return isinstance(first_artist, dict) and "id" in first_artist.keys()

    @staticmethod
    def _extract_release_date(inner_track: dict) -> Optional[datetime]:
        release_date = safe_nested_get(inner_track, [ALBUM, RELEASE_DATE])

        if isinstance(release_date, str):
            return to_datetime(release_date, SPOTIFY_RELEASE_DATE_ORDERED_FORMATS)

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return SpotifyTrack

    @property
    def name(self) -> str:
        return TRACKS
