from abc import ABC

from data_collectors.inserters.base_ids_database_inserter import BaseIDsDatabaseInserter


class BaseSpotifyDatabaseInserter(BaseIDsDatabaseInserter, ABC):
    @property
    def _serialization_method(self) -> str:
        return "from_spotify_response"
