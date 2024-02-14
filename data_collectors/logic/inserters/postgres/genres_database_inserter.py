from typing import Type, List

from genie_datastores.postgres.models import Genre

from data_collectors.logic.inserters.postgres import BaseUniqueDatabaseInserter


class GenresDatabaseInserter(BaseUniqueDatabaseInserter):
    @property
    def _unique_attributes(self) -> List[str]:
        return ["id"]

    @property
    def _orm(self) -> Type[Genre]:
        return Genre
