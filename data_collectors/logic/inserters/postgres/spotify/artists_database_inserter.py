from typing import List, Type

from genie_datastores.postgres.models import Artist
from spotipyio.logic.spotify_client import SpotifyClient
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.spotify_consts import ID
from data_collectors.logic.inserters.postgres.base_ids_database_inserter import BaseIDsDatabaseInserter
from data_collectors.utils.spotify import extract_unique_artists_ids


class ArtistsDatabaseInserter(BaseIDsDatabaseInserter):
    async def _get_raw_records(self, tracks: List[dict]) -> List[dict]:
        return [{ID: id_} for id_ in extract_unique_artists_ids(*tracks)]

    @property
    def _serialization_method(self) -> str:
        return "from_id"

    @property
    def _orm(self) -> Type[Artist]:
        return Artist

    @property
    def name(self) -> str:
        return "artists_computed_fields"
