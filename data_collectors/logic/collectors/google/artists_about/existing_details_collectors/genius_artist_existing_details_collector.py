from typing import Optional, List

from genie_common.tools import logger, AioPoolExecutor
from genie_datastores.models import DataSource
from genie_datastores.mongo.models import AboutDocument
from genie_datastores.postgres.models import Artist, GeniusArtist
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
    GeniusArtist.id.label("genius_id"),
    Artist.origin,
    Artist.birth_date,
    Artist.death_date,
    Artist.gender,
]


class GeniusArtistsExistingDetailsCollector(BaseArtistsExistingDetailsCollector):
    def __init__(self, db_engine: AsyncEngine, pool_executor: AioPoolExecutor):
        super().__init__(db_engine)
        self._pool_executor = pool_executor

    async def collect(self, limit: Optional[int]) -> List[ArtistExistingDetails]:
        rows = await self._query_relevant_genius_rows(limit)
        logger.info("Matching retrieved genius rows with about documents")
        artists_details = await self._pool_executor.run(
            iterable=rows,
            func=self._query_about_document_and_build_artist_details,
            expected_type=ArtistExistingDetails,
        )
        artists_with_about_field = [details for details in artists_details if details.about is not None]
        logger.info(f"Found about field for {len(artists_with_about_field)} out of {len(artists_details)} artists")

        return artists_details

    async def _query_relevant_genius_rows(self, limit: Optional[int]) -> List[Row]:
        logger.info(f"Querying {limit} artists about")
        query = (
            select(ARTIST_ABOUT_COLUMNS)
            .where(GeniusArtist.id == Artist.genius_id)
            .where(GeniusArtist.id.isnot(None))
            .where(Artist.id.isnot(None))
            .order_by(Artist.update_date.asc())
            .limit(limit)
        )
        cursor = await execute_query(engine=self._db_engine, query=query)

        return cursor.all()

    @property
    def data_source(self) -> DataSource:
        return DataSource.GENIUS

    @staticmethod
    async def _query_about_document_and_build_artist_details(
        row: Row,
    ) -> ArtistExistingDetails:
        document = await AboutDocument.find_one(AboutDocument.entity_id == row.genius_id)

        if document is None:
            about = None
        else:
            about = document.about

        return ArtistExistingDetails(
            id=row.id,
            about=about,
            origin=row.origin,
            birth_date=row.birth_date,
            death_date=row.death_date,
            gender=row.gender,
        )
