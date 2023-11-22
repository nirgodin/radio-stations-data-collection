from abc import abstractmethod
from typing import Iterable, Type

from postgres_client import ShazamTrack

from data_collectors.logic.inserters.base_ids_database_inserter import BaseIDsDatabaseInserter


class ShazamTracksDatabaseInserter(BaseIDsDatabaseInserter):
    @abstractmethod
    async def _get_raw_records(self, iterable: Iterable[dict]) -> Iterable[dict]:
        return iterable

    @property
    @abstractmethod
    def _serialization_method(self) -> str:
        return "from_shazam_response"

    @property
    @abstractmethod
    def _orm(self) -> Type[ShazamTrack]:
        return ShazamTrack

    @property
    @abstractmethod
    def name(self) -> str:
        return "shazam_tracks"
