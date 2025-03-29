from typing import Optional, List

from genie_common.tools import logger
from genie_datastores.postgres.models import Artist, ShazamArtist, Decision, Table
from genie_datastores.models import DataSource
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IManager
from data_collectors.logic.inserters.postgres import ChunksDatabaseInserter
from data_collectors.logic.models import DBUpdateRequest
from data_collectors.logic.updaters import ValuesDatabaseUpdater


class ShazamOriginCopyManager(IManager):
    def __init__(
        self,
        db_engine: AsyncEngine,
        db_updater: ValuesDatabaseUpdater,
        db_inserter: ChunksDatabaseInserter,
    ):
        self._db_engine = db_engine
        self._db_updater = db_updater
        self._db_inserter = db_inserter

    async def run(self, limit: Optional[int]) -> None:
        query_result = await self._query_missing_artists_origins(limit)

        if query_result:
            await self._update_artists_records(query_result)
            await self._insert_decisions_entries(query_result)

        else:
            logger.info("Did not find any record to update. Aborting")

    async def _query_missing_artists_origins(self, limit: Optional[int]) -> List[Row]:
        logger.info("Querying `shazam_artists` table for artists origin that is missing on `artists` table")
        query = (
            select(Artist.id, ShazamArtist.id.label("shazam_id"), ShazamArtist.origin)
            .where(Artist.shazam_id == ShazamArtist.id)
            .where(Artist.origin.is_(None))
            .where(ShazamArtist.origin.isnot(None))
            .limit(limit)
        )
        query_response = await execute_query(engine=self._db_engine, query=query)

        return query_response.all()

    async def _update_artists_records(self, query_result: List[Row]) -> None:
        logger.info(f"Found {len(query_result)} artists. Converting rows to update requests")
        update_requests = [self._to_update_request(row) for row in query_result]

        await self._db_updater.update(update_requests)

    @staticmethod
    def _to_update_request(row: Row) -> DBUpdateRequest:
        return DBUpdateRequest(id=row.id, values={Artist.origin: row.origin})

    async def _insert_decisions_entries(self, query_result: List[Row]) -> None:
        logger.info("Inserting decision records")
        records = []

        for row in query_result:
            decision = Decision(
                column=Artist.origin.key,
                source=DataSource.SHAZAM,
                table=Table.ARTISTS,
                table_id=row.id,
                evidence=f"shazam id: {row.shazam_id}",
            )
            records.append(decision)

        await self._db_inserter.insert(records)
