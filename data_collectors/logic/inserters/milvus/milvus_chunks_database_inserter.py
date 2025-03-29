from functools import partial
from typing import List

from genie_common.tools import ChunksGenerator, logger
from genie_datastores.milvus import MilvusClient

from data_collectors.contract import IDatabaseInserter


class MilvusChunksDatabaseInserter(IDatabaseInserter):
    def __init__(self, chunks_generator: ChunksGenerator, milvus_client: MilvusClient):
        self._chunks_generator = chunks_generator
        self._milvus_client = milvus_client

    async def insert(self, collection_name: str, records: List[dict]) -> None:
        logger.info(f"Inserting {len(records)} records")
        await self._chunks_generator.execute_by_chunk_in_parallel(
            lst=records,
            func=partial(self._insert_records_in_chunk, collection_name),
            expected_type=type(None),
        )

    async def _insert_records_in_chunk(self, collection_name: str, records: List[dict]) -> None:
        await self._milvus_client.vectors.insert(collection_name=collection_name, records=records)
