from typing import List, Type, Iterable

from genie_datastores.postgres.models import Artist

from data_collectors.logic.inserters.postgres.base_ids_database_inserter import (
    BaseIDsDatabaseInserter,
)
from data_collectors.utils.spotify import extract_unique_artists_ids


class ArtistsDatabaseInserter(BaseIDsDatabaseInserter):
    async def _get_raw_records(self, tracks: List[dict]) -> List[str]:
        return sorted(extract_unique_artists_ids(*tracks))

    def _to_records(self, raw_records: Iterable[str]) -> List[Artist]:
        return [Artist(id=id_) for id_ in raw_records]

    @property
    def _orm(self) -> Type[Artist]:
        return Artist

    @property
    def name(self) -> str:
        return "artists_computed_fields"
