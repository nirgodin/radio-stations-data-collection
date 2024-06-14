from typing import Optional, List

from genie_common.tools import logger, AioPoolExecutor
from genie_datastores.postgres.models import Artist, DataSource, SpotifyArtist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.collectors.wikipedia import WikipediaPageSummaryCollector
from data_collectors.logic.collectors.google.artists_about.base_artist_existing_details_collector import (
    BaseArtistsExistingDetailsCollector
)
from data_collectors.logic.models import ArtistExistingDetails

ARTIST_ABOUT_COLUMNS = [
    Artist.id,
    Artist.origin,
    Artist.birth_date,
    Artist.death_date,
    Artist.gender,
    SpotifyArtist.wikipedia_name,
    SpotifyArtist.wikipedia_language
]


class WikipediaArtistsExistingDetailsCollector(BaseArtistsExistingDetailsCollector):
    def __init__(self,
                 db_engine: AsyncEngine,
                 pool_executor: AioPoolExecutor,
                 wikipedia_summary_collector: WikipediaPageSummaryCollector = WikipediaPageSummaryCollector()):
        super().__init__(db_engine)
        self._pool_executor = pool_executor
        self._wikipedia_summary_collector = wikipedia_summary_collector

    async def collect(self, limit: Optional[int]) -> List[ArtistExistingDetails]:
        logger.info(f"Querying {limit} artists wikipedia pages")
        query = (
            select(ARTIST_ABOUT_COLUMNS)
            .where(SpotifyArtist.id == Artist.id)
            .where(SpotifyArtist.wikipedia_name.isnot(None))
            .order_by(Artist.update_date.asc())
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)
        rows = query_result.all()

        return await self._pool_executor.run(
            iterable=rows,
            func=self._collect_single_artist_summary,
            expected_type=ArtistExistingDetails
        )

    async def _collect_single_artist_summary(self, row: Row) -> ArtistExistingDetails:
        about = self._wikipedia_summary_collector.collect(
            name=row.wikipedia_name,
            language=row.wikipedia_language
        )

        return ArtistExistingDetails(
            id=row.id,
            about=about,
            origin=row.origin,
            birth_date=row.birth_date,
            death_date=row.death_date,
            gender=row.gender
        )

    @property
    def data_source(self) -> DataSource:
        return DataSource.GENERAL_WIKIPEDIA
