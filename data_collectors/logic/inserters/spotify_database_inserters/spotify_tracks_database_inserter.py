from typing import List, Type

from postgres_client.models.orm.spotify.base_spotify_orm_model import BaseSpotifyORMModel
from postgres_client.models.orm.spotify.spotify_track import SpotifyTrack

from data_collectors.consts.spotify_consts import TRACKS
from data_collectors.logic.inserters.spotify_database_inserters.base_spotify_database_inserter import \
    BaseSpotifyDatabaseInserter


class SpotifyTracksDatabaseInserter(BaseSpotifyDatabaseInserter):
    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        return tracks

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return SpotifyTrack

    @property
    def name(self) -> str:
        return TRACKS
