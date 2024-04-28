from typing import List, Type

from genie_datastores.postgres.models import ChartEntry

from data_collectors.logic.inserters.postgres.base_unique_database_inserter import BaseUniqueDatabaseInserter


class ChartEntriesDatabaseInserter(BaseUniqueDatabaseInserter):
    @property
    def _unique_attributes(self) -> List[str]:
        return ["key", "chart", "date"]

    @property
    def _orm(self) -> Type[ChartEntry]:
        return ChartEntry
