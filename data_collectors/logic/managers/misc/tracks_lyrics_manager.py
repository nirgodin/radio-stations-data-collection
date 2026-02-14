from collections import defaultdict
from typing import List, Optional, Dict

from genie_common.tools import logger
from genie_datastores.mongo.models import LyricsSectionDocument
from genie_datastores.postgres.models import TrackIDMapping, LyricsSection
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IManager
from data_collectors.logic.collectors import TracksLyricsFormatterCollector
from data_collectors.logic.inserters.postgres import ChunksDatabaseInserter
from data_collectors.logic.models import LyricsSourceDetails, RawLyrics


class TracksLyricsManager(IManager):
    def __init__(
        self,
        db_engine: AsyncEngine,
        prioritized_sources: List[LyricsSourceDetails],
        lyrics_formatter: TracksLyricsFormatterCollector,
        chunks_database_inserter: ChunksDatabaseInserter,
    ):
        self._db_engine = db_engine
        self._prioritized_sources = prioritized_sources
        self._lyrics_formatter = lyrics_formatter
        self._chunks_database_inserter = chunks_database_inserter

    async def run(self, limit: Optional[int]) -> None:
        logger.info("Starting to run `TracksLyricsManager`")

        for source_details in self._prioritized_sources:
            tracks_without_lyrics = await self._query_tracks_without_lyrics(source_details, limit)
            spotify_ids_to_documents = await self._collect_single_source_records(tracks_without_lyrics, source_details)
            logger.info("Inserting lyrics documents to db")

            spotify_ids_to_documents_ids = await self._insert_documents(spotify_ids_to_documents)
            await self._insert_records(spotify_ids_to_documents_ids)

    async def _query_tracks_without_lyrics(
        self, source_details: LyricsSourceDetails, limit: Optional[int]
    ) -> List[Row]:
        lyrics_sections_subquery = select(LyricsSection.track_id)
        query = (
            select(TrackIDMapping.id, source_details.column)
            .where(source_details.column.isnot(None))
            .where(TrackIDMapping.id.notin_(lyrics_sections_subquery))
            .order_by(TrackIDMapping.update_date.asc())
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.all()

    async def _collect_single_source_records(
        self, tracks_without_lyrics: List[Row], source_details: LyricsSourceDetails
    ) -> Dict[str, List[LyricsSectionDocument]]:
        logger.info(f"Collecting lyrics from `{source_details.data_source.value}` source")
        source_id_to_spotify_id = {getattr(row, source_details.column.key): row.id for row in tracks_without_lyrics}
        ids_lyrics_mapping = await source_details.collector.collect(list(source_id_to_spotify_id.keys()))
        raw_lyrics = [
            RawLyrics(id=id_, source=source_details.data_source, lyrics=lyrics)
            for id_, lyrics in ids_lyrics_mapping.items()
        ]
        documents = await self._lyrics_formatter.collect(raw_lyrics)
        records = defaultdict(list)

        for document in documents:
            spotify_id = source_id_to_spotify_id[document.entity_id]
            records[spotify_id].append(document)

        return dict(records)

    @staticmethod
    async def _insert_documents(
        spotify_ids_to_documents: Dict[str, List[LyricsSectionDocument]]
    ) -> Dict[str, List[str]]:
        spotify_id_to_documents_ids = {}

        for spotify_id, documents in spotify_ids_to_documents.items():
            insertion_result = await LyricsSectionDocument.insert_many(documents)
            spotify_id_to_documents_ids[spotify_id] = [str(object_id) for object_id in insertion_result.inserted_ids]

        return spotify_id_to_documents_ids

    async def _insert_records(self, spotify_ids_to_documents_ids: Dict[str, List[str]]) -> None:
        records = []

        for track_id, documents_ids in spotify_ids_to_documents_ids.items():
            for document_id in documents_ids:
                records.append(LyricsSection(id=document_id, track_id=track_id))

        await self._chunks_database_inserter.insert(records)
