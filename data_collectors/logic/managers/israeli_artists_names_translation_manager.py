from typing import Optional, Dict, Tuple

from genie_common.clients.google import GoogleTranslateClient
from genie_common.tools import AioPoolExecutor, logger
from genie_datastores.postgres.models import SpotifyArtist, Artist, Translation, DataSource, EntityType
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IManager
from data_collectors.logic.inserters.postgres import ChunksDatabaseInserter


class IsraeliArtistsNamesTranslationManager(IManager):
    def __init__(self,
                 db_engine: AsyncEngine,
                 pool_executor: AioPoolExecutor,
                 translation_client: GoogleTranslateClient,
                 chunks_inserter: ChunksDatabaseInserter):
        self._db_engine = db_engine
        self._pool_executor = pool_executor
        self._translation_client = translation_client
        self._chunks_inserter = chunks_inserter

    async def run(self, limit: Optional[int]) -> None:
        logger.info(f"Starting to run artists translations manager for {limit} artists")
        artists_ids_names_map = await self._query_relevant_artists(limit)

        if not artists_ids_names_map:
            logger.info("Did not find any relevant artist to translate. Aborting")
            return

        logger.info(f"Translating {len(artists_ids_names_map)} artists names")
        records = await self._pool_executor.run(
            iterable=artists_ids_names_map.items(),
            func=self._translate_single_record,
            expected_type=Translation
        )
        await self._chunks_inserter.insert(records)

    async def _query_relevant_artists(self, limit: Optional[int]) -> Dict[str, str]:
        logger.info(f"Querying database for relevant artists names")
        translations_subquery = (
            select(Translation.id)
            .where(Translation.entity_source == DataSource.SPOTIFY)
            .where(Translation.entity_type == EntityType.ARTIST)
        )
        query = (
            select(SpotifyArtist.id, SpotifyArtist.name)
            .where(SpotifyArtist.id == Artist.id)
            .where(Artist.is_israeli.is_(True))
            .where(SpotifyArtist.id.notin_(translations_subquery))
            .where(SpotifyArtist.name.regexp_match(r'[a-zA-Z]'))
            .limit(limit)
        )
        raw_result = await execute_query(engine=self._db_engine, query=query)
        query_result = raw_result.all()

        return {artist.id: artist.name for artist in query_result}

    async def _translate_single_record(self, artist_id_and_name: Tuple[str, str]) -> Translation:
        artist_id, artist_name = artist_id_and_name
        translation_response = await self._translation_client.translate(
            texts=[artist_name],
            target_language="he",
            source_language="en"
        )

        if translation_response:
            first_translation = translation_response[0]
            return Translation(
                id=artist_id,
                entity_source=DataSource.SPOTIFY,
                entity_type=EntityType.ARTIST,
                text=artist_name,
                translation=first_translation.translation,
                source_language=first_translation.source_language,
                target_language=first_translation.target_language
            )
