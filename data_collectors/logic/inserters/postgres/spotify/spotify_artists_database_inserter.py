from typing import List, Type, Optional

from genie_datastores.postgres.models import SpotifyArtist
from genie_datastores.postgres.models.orm.spotify.base_spotify_orm_model import (
    BaseSpotifyORMModel,
)
from spotipyio import SpotifyClient
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.spotify_consts import ARTISTS, ID, NAME, GENRES
from data_collectors.logic.inserters.postgres import BaseIDsDatabaseInserter, ChunksDatabaseInserter
from data_collectors.utils.spotify import extract_unique_artists_ids


class SpotifyArtistsDatabaseInserter(BaseIDsDatabaseInserter):
    def __init__(self, db_engine: AsyncEngine, chunks_inserter: ChunksDatabaseInserter, spotify_client: SpotifyClient):
        super().__init__(db_engine, chunks_inserter)
        self._spotify_client = spotify_client

    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        artists_ids = extract_unique_artists_ids(*tracks)
        valid_artists_ids = [id_ for id_ in artists_ids if isinstance(id_, str)]

        return await self._spotify_client.artists.info.run(sorted(valid_artists_ids))

    def _to_record(self, raw_record: dict) -> SpotifyArtist:
        return SpotifyArtist(id=raw_record[ID], name=raw_record[NAME], genres=self._extract_genres(raw_record))

    @staticmethod
    def _extract_genres(raw_record: dict) -> Optional[List[str]]:
        genres = raw_record.get(GENRES)
        return genres if genres else None

    @property
    def _orm(self) -> Type[BaseSpotifyORMModel]:
        return SpotifyArtist

    @property
    def name(self) -> str:
        return ARTISTS
