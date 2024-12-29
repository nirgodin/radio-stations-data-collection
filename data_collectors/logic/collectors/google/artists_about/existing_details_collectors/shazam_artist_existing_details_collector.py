from typing import Optional, List

from genie_common.tools import logger, AioPoolExecutor
from genie_datastores.models import DataSource
from genie_datastores.mongo.models import AboutDocument
from genie_datastores.postgres.models import Artist, ShazamArtist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.collectors.google.artists_about.base_artist_existing_details_collector import (
    BaseArtistsExistingDetailsCollector,
)
from data_collectors.logic.models import ArtistExistingDetails

ARTIST_ABOUT_COLUMNS = [
    Artist.id,
    Artist.shazam_id,
    Artist.origin,
    Artist.birth_date,
    Artist.death_date,
    Artist.gender,
]


class ShazamArtistsExistingDetailsCollector(BaseArtistsExistingDetailsCollector):
    def __init__(self, db_engine: AsyncEngine, pool_executor: AioPoolExecutor):
        super().__init__(db_engine)
        self._pool_executor = pool_executor

    async def collect(self, limit: Optional[int]) -> List[ArtistExistingDetails]:
        rows = await self._query_database_for_relevant_artists(limit)
        return await self._pool_executor.run(
            iterable=rows,
            func=self._create_single_artist_existing_details,
            expected_type=ArtistExistingDetails,
        )

    async def _query_database_for_relevant_artists(
        self, limit: Optional[int]
    ) -> List[Row]:
        logger.info(f"Querying {limit} artists about")
        query = (
            select(ARTIST_ABOUT_COLUMNS)
            .where(ShazamArtist.id == Artist.shazam_id)
            .where(ShazamArtist.has_about_document.is_(True))
            .where(Artist.id.isnot(None))
            .order_by(Artist.update_date.asc())
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.all()

    async def _create_single_artist_existing_details(
        self, row: Row
    ) -> Optional[ArtistExistingDetails]:
        document = await AboutDocument.find_one(
            AboutDocument.entity_id == row.shazam_id,
            AboutDocument.source == self.data_source,
        )

        if document:
            return ArtistExistingDetails.from_row(row, document.about)

    @property
    def data_source(self) -> DataSource:
        return DataSource.SHAZAM
