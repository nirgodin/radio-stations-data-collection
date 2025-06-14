from typing import Optional, Dict

from genie_common.tools import logger
from genie_datastores.postgres.models import SpotifyArtist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors import ValuesDatabaseUpdater
from data_collectors.contract import IManager
from data_collectors.logic.collectors import GoogleArtistsWebPagesCollector


class GoogleArtistsWebPagesManager(IManager):
    def __init__(
        self,
        db_engine: AsyncEngine,
        web_pages_collector: GoogleArtistsWebPagesCollector,
        db_updater: ValuesDatabaseUpdater,
    ):
        self._db_engine = db_engine
        self._web_pages_collector = web_pages_collector
        self._db_updater = db_updater

    async def run(self, limit: Optional[int]) -> None:
        artists_ids_names_map = await self._query_artists_with_missing_web_pages(limit)
        web_pages = await self._web_pages_collector.collect(artists_ids_names_map)
        print("b")

    async def _query_artists_with_missing_web_pages(self, limit: Optional[int]) -> Dict[str, str]:
        logger.info(f"Querying {limit} artists with missing web pages")
        query = (
            select(SpotifyArtist.id, SpotifyArtist.name)
            .where(SpotifyArtist.wikipedia_name.is_(None))
            .order_by(SpotifyArtist.update_date.asc())
            .limit(limit)
        )
        query_result = await execute_query(self._db_engine, query)

        return {row.id: row.name for row in query_result.all()}
