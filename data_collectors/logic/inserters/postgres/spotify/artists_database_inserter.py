from typing import List, Type

from genie_datastores.postgres.models import Artist

from data_collectors.logic.inserters.postgres.base_ids_database_inserter import (
    BaseIDsDatabaseInserter,
)
from data_collectors.utils.spotify import extract_unique_artists_ids


class ArtistsDatabaseInserter(BaseIDsDatabaseInserter):
    async def _get_raw_records(self, tracks: List[dict]) -> List[str]:
        artists_ids = extract_unique_artists_ids(*tracks)
        valid_artists_ids = [id_ for id_ in artists_ids if isinstance(id_, str)]

        return sorted(valid_artists_ids)

    def _to_record(self, raw_record: str) -> Artist:
        return Artist(id=raw_record)

    @property
    def _orm(self) -> Type[Artist]:
        return Artist

    @property
    def name(self) -> str:
        return "artists_computed_fields"
