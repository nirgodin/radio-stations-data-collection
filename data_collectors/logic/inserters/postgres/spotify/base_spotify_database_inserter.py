from abc import ABC

from data_collectors.logic.inserters.postgres.base_ids_database_inserter import BaseIDsDatabaseInserter


class BaseSpotifyDatabaseInserter(BaseIDsDatabaseInserter, ABC):
    @property
    def _serialization_method(self) -> str:
        return "from_spotify_response"
