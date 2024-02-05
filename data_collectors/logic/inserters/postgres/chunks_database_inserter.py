from typing import List

from genie_common.tools import logger, ChunksGenerator
from genie_datastores.postgres.models import BaseORMModel
from genie_datastores.postgres.operations import insert_records
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IPostgresDatabaseInserter


class ChunksDatabaseInserter(IPostgresDatabaseInserter):
    def __init__(self, db_engine: AsyncEngine, chunks_generator: ChunksGenerator):
        super().__init__(db_engine)
        self._chunks_generator = chunks_generator

    async def insert(self, records: List[BaseORMModel]) -> None:
        logger.info(f"Inserting {len(records)} records")
        await self._chunks_generator.execute_by_chunk_in_parallel(
            lst=records,
            func=self._insert_records_in_chunk,
            expected_type=type(None)
        )

    async def _insert_records_in_chunk(self, records: List[BaseORMModel]) -> None:
        try:
            await insert_records(engine=self._db_engine, records=records)

        except IntegrityError:
            logger.exception("Failed to insert records in one chunk. Trying to insert one by one")
            await self._insert_records_one_by_one(records)

    async def _insert_records_one_by_one(self, records: List[BaseORMModel]) -> None:
        success_count = 0

        for record in records:
            try:
                await insert_records(engine=self._db_engine, records=[record])
                success_count += 1

            except IntegrityError:
                logger.exception("Failed to insert record! skipping")

        logger.info(f"Successfully inserted {success_count} out of {len(records)}")
