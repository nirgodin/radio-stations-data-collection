from typing import List, Type, Any

from genie_datastores.postgres.models.orm.spotify.base_spotify_orm_model import (
    BaseSpotifyORMModel,
)
from genie_datastores.postgres.models import SpotifyTrack

from data_collectors.consts.spotify_consts import TRACKS, TRACK
from data_collectors.logic.inserters.postgres.spotify.base_spotify_database_inserter import (
    BaseSpotifyDatabaseInserter,
)


class SpotifyTracksDatabaseInserter(BaseSpotifyDatabaseInserter):
    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        return tracks

    def _is_serializable(self, raw: Any) -> bool:
        if isinstance(raw, dict):
            inner_track = raw.get(TRACK)

            if isinstance(inner_track, dict) and self._contains_required_fields(inner_track):
                return self._has_primary_artist(inner_track)

        return False

    @staticmethod
    def _contains_required_fields(inner_track: dict) -> bool:
        return all(key in inner_track.keys() for key in ["id", "name", "explicit", "track_number"])

    @staticmethod
    def _has_primary_artist(inner_track: dict) -> bool:
        artists = inner_track.get("artists")

        if isinstance(artists, list) and len(artists) > 0:
            first_artist = artists[0]
            return isinstance(first_artist, dict) and "id" in first_artist.keys()

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return SpotifyTrack

    @property
    def name(self) -> str:
        return TRACKS
