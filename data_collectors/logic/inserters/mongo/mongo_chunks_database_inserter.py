from typing import List, Type

from genie_common.tools import ChunksGenerator, logger
from genie_datastores.contract import IDatabaseInserter
from genie_datastores.mongo.models import BaseDocument


class MongoChunksDatabaseInserter(IDatabaseInserter):
    def __init__(self, chunks_generator: ChunksGenerator):
        self._chunks_generator = chunks_generator

    async def insert(self, records: List[BaseDocument], model: Type[BaseDocument]) -> None:
        logger.info(f"Inserting {len(records)} by chunks to MongoDB `{model.__name__}` collection")
        await self._chunks_generator.execute_by_chunk_in_parallel(
            lst=records,
            func=model.insert_many,
            expected_type=type(None)
        )
