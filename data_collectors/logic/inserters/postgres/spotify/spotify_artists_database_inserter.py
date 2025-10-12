from typing import List, Type, Optional

from genie_datastores.postgres.models import SpotifyArtist
from genie_datastores.postgres.models.orm.spotify.base_spotify_orm_model import (
    BaseSpotifyORMModel,
)
from spotipyio import SpotifyClient
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.spotify_consts import ARTISTS, ID, NAME, GENRES
from data_collectors.logic.inserters.postgres import BaseIDsDatabaseInserter
from data_collectors.utils.spotify import extract_unique_artists_ids


class SpotifyArtistsDatabaseInserter(BaseIDsDatabaseInserter):
    def __init__(self, db_engine: AsyncEngine, spotify_client: SpotifyClient):
        super().__init__(db_engine)
        self._spotify_client = spotify_client

    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        artists_ids = extract_unique_artists_ids(*tracks)
        return await self._spotify_client.artists.info.run(sorted(artists_ids))

    def _to_records(self, raw_records: List[dict]) -> List[SpotifyArtist]:
        return [self._to_record(response) for response in raw_records]

    def _to_record(self, response: dict) -> SpotifyArtist:
        return SpotifyArtist(id=response[ID], name=response[NAME], genres=self._extract_genres(response))

    @staticmethod
    def _extract_genres(response: dict) -> Optional[List[str]]:
        genres = response.get(GENRES)
        return genres if genres else None

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return SpotifyArtist

    @property
    def name(self) -> str:
        return ARTISTS
