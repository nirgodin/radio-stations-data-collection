from typing import Optional, List

from genie_common.tools import logger
from genie_datastores.postgres.models import Artist, ShazamArtist, Gender, DataSource
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.updaters import ArtistsDatabaseUpdater
from data_collectors.contract import IManager
from data_collectors.logic.models import DBUpdateRequest


class ShazamBirthDateCopyManager(IManager):
    def __init__(self, db_engine: AsyncEngine, artists_updater: ArtistsDatabaseUpdater):
        self._db_engine = db_engine
        self._artists_updater = artists_updater

    async def run(self, limit: Optional[int]) -> None:
        query_result = await self._query_missing_artists_birth_dates()

        if query_result:
            logger.info(f"Found {len(query_result)} artists. Converting rows to update requests")
            update_requests = [self._to_update_request(row) for row in query_result]

            await self._artists_updater.update(update_requests)

    async def _query_missing_artists_birth_dates(self) -> List[Row]:
        logger.info("Querying `shazam_artists` table for artists birth date that are missing on `artists` table")
        query = (
            select(Artist.id, ShazamArtist.birth_date)
            .where(Artist.shazam_id == ShazamArtist.id)
            .where(Artist.birth_date.is_(None))
            .where(Artist.death_date.is_(None))
            .where(Artist.gender.in_([Gender.MALE, Gender.FEMALE]))
            .where(ShazamArtist.birth_date.isnot(None))
        )
        query_response = await execute_query(engine=self._db_engine, query=query)

        return query_response.all()

    @staticmethod
    def _to_update_request(row: Row) -> DBUpdateRequest:
        return DBUpdateRequest(
            id=row.id,
            values={
                Artist.birth_date: row.birth_date,
                Artist.birth_date_source: DataSource.SHAZAM
            }
        )
