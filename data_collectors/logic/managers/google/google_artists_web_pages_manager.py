from typing import Optional, Dict

from genie_common.tools import logger
from genie_datastores.postgres.models import SpotifyArtist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IManager
from data_collectors.logic.collectors import GoogleArtistsWebPagesCollector
from data_collectors.logic.serializers import ArtistsWebPagesSerializer
from data_collectors.logic.updaters import ValuesDatabaseUpdater


class GoogleArtistsWebPagesManager(IManager):
    def __init__(
        self,
        db_engine: AsyncEngine,
        web_pages_collector: GoogleArtistsWebPagesCollector,
        web_pages_serializer: ArtistsWebPagesSerializer,
        db_updater: ValuesDatabaseUpdater,
    ):
        self._db_engine = db_engine
        self._web_pages_collector = web_pages_collector
        self._web_pages_serializer = web_pages_serializer
        self._db_updater = db_updater

    async def run(self, limit: Optional[int]) -> None:
        ids_to_artists_map = await self._query_artists_with_missing_web_pages(limit)
        artists_ids_names_map = {artist.id: artist.name for artist in ids_to_artists_map.values()}
        artist_id_web_pages_map = await self._web_pages_collector.collect(artists_ids_names_map)
        update_requests = self._web_pages_serializer.serialize(ids_to_artists_map, artist_id_web_pages_map)

        await self._db_updater.update(update_requests)

    async def _query_artists_with_missing_web_pages(self, limit: Optional[int]) -> Dict[str, SpotifyArtist]:
        logger.info(f"Querying {limit} artists with missing web pages")
        query = (
            select(SpotifyArtist)
            .where(SpotifyArtist.wikipedia_name.is_(None))
            .order_by(SpotifyArtist.update_date.asc())
            .limit(limit)
        )
        query_result = await execute_query(self._db_engine, query)

        return {row.id: row for row in query_result.scalars().all()}
