from typing import Optional, List

from genie_common.tools import logger
from genie_common.utils import safe_nested_get
from genie_datastores.models import DataSource, EntityType
from genie_datastores.mongo.models import AboutDocument
from genie_datastores.postgres.models import GeniusArtist, Artist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.genius_consts import (
    ALTERNATE_NAMES,
    FACEBOOK_NAME,
    INSTAGRAM_NAME,
    TWITTER_NAME,
    DESCRIPTION,
)
from data_collectors.consts.spotify_consts import ID, NAME
from data_collectors.contract import IManager
from data_collectors.logic.collectors import GeniusArtistsCollector
from data_collectors.logic.inserters.postgres import ChunksDatabaseInserter
from data_collectors.logic.models import GeniusTextFormat


class GeniusArtistsManager(IManager):
    def __init__(
        self,
        db_engine: AsyncEngine,
        artists_collector: GeniusArtistsCollector,
        chunks_inserter: ChunksDatabaseInserter,
        text_format: GeniusTextFormat,
    ):
        self._db_engine = db_engine
        self._artists_collector = artists_collector
        self._chunks_inserter = chunks_inserter
        self._text_format = text_format

    async def run(self, limit: Optional[int]) -> None:
        logger.info(f"Starting to run `GeniusArtistsManager` for {limit} entities")
        artists_ids = await self._query_missing_artists_ids(limit)

        if not artists_ids:
            logger.info("Did not find any missing artist. Aborting.")
            return

        await self._query_and_insert_artists_data(artists_ids)

    async def _query_missing_artists_ids(self, limit: Optional[int]) -> List[str]:
        logger.info(f"Querying {limit} artists without genius ids from database")
        existing_genius_ids_subquery = select(GeniusArtist.id)
        query = (
            select(Artist.genius_id)
            .where(Artist.genius_id.isnot(None))
            .where(Artist.genius_id.notin_(existing_genius_ids_subquery))
            .limit(limit)
        )
        cursor = await execute_query(engine=self._db_engine, query=query)

        return cursor.scalars().all()

    async def _query_and_insert_artists_data(self, artists_ids: List[str]) -> None:
        artists = await self._artists_collector.collect(ids=artists_ids, text_format=self._text_format)
        valid_artists = [artist for artist in artists if artist[NAME].lower() not in ["genius", "spotify"]]
        await self._insert_artists_about_documents(valid_artists)
        await self._insert_genius_artists_records(valid_artists)

    async def _insert_genius_artists_records(self, artists: List[dict]) -> None:
        logger.info(f"Inserting {len(artists)} GeniusArtist records")
        records = []

        for artist in artists:
            record = GeniusArtist(
                id=str(artist[ID]),
                name=artist[NAME],
                alternate_names=artist.get(ALTERNATE_NAMES),
                facebook_name=artist.get(FACEBOOK_NAME),
                instagram_name=artist.get(INSTAGRAM_NAME),
                twitter_name=artist.get(TWITTER_NAME),
            )
            records.append(record)

        await self._chunks_inserter.insert(records)

    async def _insert_artists_about_documents(self, artists: List[dict]) -> None:
        logger.info("Extracting artists description documents")
        documents = self._create_about_documents(artists)

        if not documents:
            logger.info("Did not find any valid description. Aborting documents insertion")
            return

        logger.info(f"Inserting {len(documents)} artists description documents")
        await AboutDocument.insert_many(documents)

    def _create_about_documents(self, artists: List[dict]) -> List[AboutDocument]:
        documents = []

        for artist in artists:
            description = self._extract_single_document_description(artist)

            if description is not None:
                document = AboutDocument(
                    about=description,
                    entity_type=EntityType.ARTIST,
                    entity_id=str(artist[ID]),
                    name=artist[NAME],
                    source=DataSource.GENIUS,
                )
                documents.append(document)

        return documents

    def _extract_single_document_description(self, artist: dict) -> Optional[str]:
        description = safe_nested_get(artist, [DESCRIPTION, self._text_format.value])

        if description and description != "?":
            return description
