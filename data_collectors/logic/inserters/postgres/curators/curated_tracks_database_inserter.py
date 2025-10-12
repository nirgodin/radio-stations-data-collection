from typing import List, Type

from genie_datastores.postgres.models import ChartEntry, CuratedTrack

from data_collectors.logic.inserters.postgres.base_unique_database_inserter import (
    BaseUniqueDatabaseInserter,
)


class CuratedTracksDatabaseInserter(BaseUniqueDatabaseInserter):
    @property
    def _unique_attributes(self) -> List[str]:
        return [CuratedTrack.track_id.key, CuratedTrack.collection.key]

    @property
    def _orm(self) -> Type[ChartEntry]:
        return CuratedTrack
