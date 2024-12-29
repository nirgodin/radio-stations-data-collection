from typing import List, Type

from genie_datastores.postgres.models import RadioTrack

from data_collectors.logic.inserters.postgres.base_unique_database_inserter import (
    BaseUniqueDatabaseInserter,
)


class RadioTracksDatabaseInserter(BaseUniqueDatabaseInserter):
    @property
    def _unique_attributes(self) -> List[str]:
        return ["track_id", "added_at", "station"]

    @property
    def _orm(self) -> Type[RadioTrack]:
        return RadioTrack
