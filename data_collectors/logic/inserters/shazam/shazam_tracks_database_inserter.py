from abc import abstractmethod
from typing import Iterable, Type

from postgres_client import ShazamTrack

from data_collectors.logic.inserters.base_ids_database_inserter import BaseIDsDatabaseInserter


class ShazamTracksDatabaseInserter(BaseIDsDatabaseInserter):
    async def _get_raw_records(self, iterable: Iterable[dict]) -> Iterable[dict]:
        return iterable

    @property
    def _serialization_method(self) -> str:
        return "from_shazam_response"

    @property
    def _orm(self) -> Type[ShazamTrack]:
        return ShazamTrack

    @property
    def name(self) -> str:
        return "shazam_tracks"