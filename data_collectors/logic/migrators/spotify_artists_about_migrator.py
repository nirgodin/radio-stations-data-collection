import json
from typing import Generator, List

from genie_common.tools import logger
from genie_datastores.mongo.models import AboutDocument
from genie_datastores.postgres.models import SpotifyArtist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.engine import Result, ChunkedIteratorResult
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors import ValuesDatabaseUpdater
from data_collectors.logic.models import SpotifyArtistAbout


class SpotifyArtistsAboutMigrator:
    def __init__(self, db_engine: AsyncEngine, db_updater: ValuesDatabaseUpdater, chunk_size: int = 200):
        self._db_engine = db_engine
        self._chunk_size = chunk_size
        self._db_updater = db_updater
        self._exceptions = []

    async def migrate(self):
        cursor = await self._query_postgres_for_abouts()
        chunks = self._generate_abouts_chunks(cursor)
        await self._execute_migrations_by_chunk(chunks)
        self._save_exceptions_to_file()

    async def _query_postgres_for_abouts(self) -> ChunkedIteratorResult:
        logger.info("Querying database for existing artists about documents")
        query = (
            select(SpotifyArtist.id, SpotifyArtist.name, SpotifyArtist.about)
            .where(SpotifyArtist.about.isnot(None))
        )

        return await execute_query(engine=self._db_engine, query=query)

    def _generate_abouts_chunks(self, cursor: ChunkedIteratorResult) -> Generator[List[SpotifyArtistAbout], None, None]:
        chunk = []

        for row in cursor:
            about = SpotifyArtistAbout(
                id=row.id,
                name=row.name,
                about=row.about
            )
            chunk.append(about)

            if len(chunk) >= self._chunk_size:
                yield chunk
                chunk = []

        if chunk:
            yield chunk

    async def _execute_migrations_by_chunk(self, chunks: Generator[List[SpotifyArtistAbout], None, None]) -> None:
        chunk_number = 1

        for chunk in chunks:
            logger.info(f"Executing chunk number {chunk_number}")

            try:
                await self._execute_single_chunk(chunk)
            except Exception as e:
                logger.exception(f"Received exception during chunk {chunk_number} insertion")
                self._save_exception_status(chunk, e)

            chunk_number += 1

    async def _execute_single_chunk(self, chunk: List[SpotifyArtistAbout]) -> None:
        documents = [record.to_about_document() for record in chunk]
        logger.info(f"Inserting {len(documents)} about documents")
        await AboutDocument.insert_many(documents)
        logger.info(f"Successfully inserted {len(documents)} about documents")
        update_requests = [record.to_existing_about_document_update_request() for record in chunk]
        await self._db_updater.update(update_requests)

    def _save_exception_status(self, chunk: List[SpotifyArtistAbout], exception: Exception) -> None:
        ids = [record.id for record in chunk]
        details = {
            "ids": ids,
            "exception": str(exception)
        }
        self._exceptions.append(details)

    def _save_exceptions_to_file(self):
        with open("spotify_artists_about_exceptions.json", "w") as f:
            json.dump(self._exceptions, f, indent=4)
