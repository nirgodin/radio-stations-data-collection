from typing import Optional, List

from genie_common.tools import AioPoolExecutor, logger
from genie_datastores.models import DataSource
from genie_datastores.mongo.models import AboutDocument
from genie_datastores.postgres.models import SpotifyArtist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.collectors import WikipediaPageSummaryCollector
from data_collectors.contract import IManager
from data_collectors.logic.models import WikipediaArtistAbout


class WikipediaArtistsAboutManager(IManager):
    def __init__(self,
                 db_engine: AsyncEngine,
                 pool_executor: AioPoolExecutor,
                 page_summary_collector: WikipediaPageSummaryCollector = WikipediaPageSummaryCollector()):
        self._db_engine = db_engine
        self._pool_executor = pool_executor
        self._page_summary_collector = page_summary_collector

    async def run(self, limit: Optional[int]) -> None:
        logger.info(f"Starting to run WikipediaArtistsAboutDocumentManager for {limit} artists")
        raw_artists_details = await self._retrieve_artists_raw_details(limit)
        logger.info(f"Collecting {len(raw_artists_details)} artists Wikipedia page summaries")
        artists_details = await self._pool_executor.run(
            iterable=raw_artists_details,
            func=self._collect_artist_wiki_summary,
            expected_type=WikipediaArtistAbout
        )
        logger.info(f"Inserting Wikipedia summary documents for {len(artists_details)} artists summaries")

        await self._insert_wikipedia_summary_documents(artists_details)

    async def _retrieve_artists_raw_details(self, limit: Optional[int]) -> List[WikipediaArtistAbout]:
        query = (
            select(SpotifyArtist.id, SpotifyArtist.name, SpotifyArtist.wikipedia_name, SpotifyArtist.wikipedia_language)
            .where(SpotifyArtist.wikipedia_name.isnot(None))
            .order_by(SpotifyArtist.update_date.asc())
            .limit(limit)
        )
        cursor = await execute_query(engine=self._db_engine, query=query)
        query_result = cursor.all()

        return [WikipediaArtistAbout.from_row(row) for row in query_result]

    async def _collect_artist_wiki_summary(self, artist_details: WikipediaArtistAbout) -> Optional[WikipediaArtistAbout]:
        summary = await self._page_summary_collector.collect(
            name=artist_details.wikipedia_name,
            language=artist_details.wikipedia_language
        )
        artist_details.about = summary

        return artist_details

    async def _insert_wikipedia_summary_documents(self, artists_details: List[WikipediaArtistAbout]) -> None:
        results = await self._pool_executor.run(
            iterable=artists_details,
            func=self._insert_single_summary_document,
            expected_type=bool
        )
        number_of_successful_results = len(results)
        inserted_documents = [result for result in results if result is True]
        number_of_inserted_documents = len(inserted_documents)
        number_of_non_inserted_documents = number_of_successful_results - number_of_inserted_documents

        logger.info(
            f"Out of {number_of_successful_results} results, {number_of_inserted_documents} wre inserted as new "
            f"documents, and {number_of_non_inserted_documents} were not inserted because they already exist"
        )

    @staticmethod
    async def _insert_single_summary_document(artist_details: WikipediaArtistAbout) -> bool:
        if artist_details.about.strip() != "":
            existing_document = await AboutDocument.find_one(
                AboutDocument.entity_id == artist_details.id,
                AboutDocument.source == DataSource.WIKIPEDIA
            )

            if existing_document is None:
                document = artist_details.to_about_document()
                await AboutDocument.insert_one(document)

                return True

        return False
