from typing import List

from genie_datastores.postgres.models import BaseORMModel

from data_collectors.contract import BasePostgresDatabaseInserter


class TrackNamesEmbeddingsInserter(BasePostgresDatabaseInserter):
    async def insert(self) -> List[BaseORMModel]:
        pass
