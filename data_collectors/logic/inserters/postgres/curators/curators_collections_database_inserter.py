from typing import Type, Iterable

from genie_datastores.postgres.models import CuratorCollection

from data_collectors.logic.inserters.postgres import BaseIDsDatabaseInserter
from data_collectors.logic.models import Curation


class CuratorsCollectionsDatabaseInserter(BaseIDsDatabaseInserter):
    async def _get_raw_records(self, curations: Iterable[Curation]) -> Iterable[Curation]:
        return curations

    def _to_record(self, curation: Curation) -> CuratorCollection:
        return curation.to_collection()

    @property
    def _orm(self) -> Type[CuratorCollection]:
        return CuratorCollection

    @property
    def name(self) -> str:
        return "curations"
