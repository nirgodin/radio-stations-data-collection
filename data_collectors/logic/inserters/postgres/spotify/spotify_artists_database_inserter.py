from typing import List, Type

from genie_datastores.postgres.models import SpotifyArtist
from genie_datastores.postgres.models.orm.spotify.base_spotify_orm_model import BaseSpotifyORMModel
from spotipyio import SpotifyClient
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.spotify_consts import ARTISTS
from data_collectors.logic.inserters.postgres.spotify.base_spotify_database_inserter import \
    BaseSpotifyDatabaseInserter
from data_collectors.utils.spotify import extract_unique_artists_ids


class SpotifyArtistsDatabaseInserter(BaseSpotifyDatabaseInserter):
    def __init__(self, db_engine: AsyncEngine, spotify_client: SpotifyClient):
        super().__init__(db_engine)
        self._spotify_client = spotify_client

    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        artists_ids = extract_unique_artists_ids(*tracks)
        return await self._spotify_client.artists.info.run(list(artists_ids))

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return SpotifyArtist

    @property
    def name(self) -> str:
        return ARTISTS
