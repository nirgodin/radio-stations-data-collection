from typing import List, Optional, Type

from genie_datastores.postgres.models.orm.spotify.base_spotify_orm_model import BaseSpotifyORMModel
from genie_datastores.postgres.models import SpotifyArtist
from spotipyio.logic.spotify_client import SpotifyClient
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.spotify_consts import TRACK, ARTISTS, ID
from data_collectors.logic.inserters.postgres.spotify.base_spotify_database_inserter import \
    BaseSpotifyDatabaseInserter


class SpotifyArtistsDatabaseInserter(BaseSpotifyDatabaseInserter):
    def __init__(self, db_engine: AsyncEngine, spotify_client: SpotifyClient):
        super().__init__(db_engine)
        self._spotify_client = spotify_client

    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        artists_ids = set()

        for track in tracks:
            artist_id = self._get_single_artist_id(track)

            if artist_id is not None:
                artists_ids.add(artist_id)

        return await self._spotify_client.artists.info.run(list(artists_ids))

    @staticmethod
    def _get_single_artist_id(track: dict) -> Optional[str]:
        inner_track = track.get(TRACK, {})
        if inner_track is None:
            return

        artists = inner_track.get(ARTISTS, [])
        if not artists:
            return

        return artists[0][ID]

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return SpotifyArtist

    @property
    def name(self) -> str:
        return ARTISTS
