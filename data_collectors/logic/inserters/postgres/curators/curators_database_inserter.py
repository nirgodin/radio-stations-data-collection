from typing import Type, Iterable

from genie_datastores.postgres.models import Curator

from data_collectors.logic.inserters.postgres import BaseIDsDatabaseInserter
from data_collectors.logic.models import Curation


class CuratorsDatabaseInserter(BaseIDsDatabaseInserter):
    async def _get_raw_records(self, curations: Iterable[Curation]) -> Iterable[Curation]:
        return curations

    def _to_record(self, curation: Curation) -> Curator:
        return curation.to_curator()

    @property
    def _orm(self) -> Type[Curator]:
        return Curator

    @property
    def name(self) -> str:
        return "curations"
