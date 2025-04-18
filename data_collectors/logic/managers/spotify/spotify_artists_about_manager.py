from typing import Optional, List, Dict

from genie_common.tools import logger
from genie_datastores.mongo.models import AboutDocument
from genie_datastores.postgres.models import SpotifyArtist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.updaters import ValuesDatabaseUpdater
from data_collectors.logic.collectors import SpotifyArtistsAboutCollector
from data_collectors.contract import IManager
from data_collectors.logic.models import SpotifyArtistAbout


class SpotifyArtistsAboutManager(IManager):
    def __init__(
        self,
        db_engine: AsyncEngine,
        abouts_collector: SpotifyArtistsAboutCollector,
        db_updater: ValuesDatabaseUpdater,
    ):
        self._abouts_collector = abouts_collector
        self._db_engine = db_engine
        self._db_updater = db_updater

    async def run(self, limit: Optional[int]) -> None:
        artists_ids_names_map = await self._query_missing_artists_ids(limit)
        abouts = await self._abouts_collector.collect(artists_ids_names_map)
        await self._update_social_media_fields(abouts)
        await self._insert_about_documents(abouts)

    async def _query_missing_artists_ids(self, limit: Optional[int]) -> Dict[str, str]:
        logger.info("Querying database for artists with missing about field")
        query = (
            select(SpotifyArtist.id, SpotifyArtist.name)
            .where(SpotifyArtist.has_about_document.is_(False))
            .order_by(SpotifyArtist.update_date.asc())
            .limit(limit)
        )
        cursor = await execute_query(engine=self._db_engine, query=query)
        query_result = cursor.all()

        return {row.id: row.name for row in query_result}

    async def _update_social_media_fields(self, abouts: List[SpotifyArtistAbout]) -> None:
        logger.info("Updating spotify artists social media fields")
        update_requests = [about.to_social_media_update_request() for about in abouts]

        await self._db_updater.update(update_requests)

    async def _insert_about_documents(self, abouts: List[SpotifyArtistAbout]) -> None:
        relevant_abouts = [record for record in abouts if record.about is not None]
        await self._update_existing_about_document(relevant_abouts)
        documents = [about.to_about_document() for about in relevant_abouts]

        if documents:
            logger.info(f"Inserting {len(documents)} about documents")
            await AboutDocument.insert_many(documents)

        else:
            logger.warning("Did not find any about document. Skipping documents insertion")

    async def _update_existing_about_document(self, abouts: List[SpotifyArtistAbout]) -> None:
        logger.info(f"Updating database about document exist for {len(abouts)} records")
        update_requests = [about.to_existing_about_document_update_request() for about in abouts]

        await self._db_updater.update(update_requests)
