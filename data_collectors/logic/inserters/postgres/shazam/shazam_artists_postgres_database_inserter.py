from typing import Iterable, Type

from genie_datastores.postgres.models import ShazamArtist

from data_collectors.logic.inserters.postgres.base_ids_database_inserter import (
    BaseIDsDatabaseInserter,
)


class ShazamArtistsPostgresDatabaseInserter(BaseIDsDatabaseInserter):
    async def _get_raw_records(self, iterable: Iterable[dict]) -> Iterable[dict]:
        return iterable

    @property
    def _serialization_method(self) -> str:
        return "from_shazam_response"

    @property
    def _orm(self) -> Type[ShazamArtist]:
        return ShazamArtist

    @property
    def name(self) -> str:
        return "shazam_artists"
