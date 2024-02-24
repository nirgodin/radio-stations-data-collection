from typing import List, Optional

from genie_common.tools import logger
from genie_datastores.postgres.models import TrackIDMapping, TrackLyrics
from genie_datastores.postgres.operations import execute_query, insert_records
from sqlalchemy import select, or_
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IManager
from data_collectors.logic.models import LyricsSourceDetails
from data_collectors.logic.serializers import TracksLyricsSerializer


class TracksLyricsManager(IManager):
    def __init__(self,
                 db_engine: AsyncEngine,
                 prioritized_sources: List[LyricsSourceDetails],
                 records_serializer: TracksLyricsSerializer = TracksLyricsSerializer()):
        self._db_engine = db_engine
        self._prioritized_sources = prioritized_sources
        self._records_serializer = records_serializer

    async def run(self, limit: Optional[int]) -> None:
        logger.info("Starting to run `TracksLyricsManager`")
        tracks_without_lyrics = await self._query_tracks_without_lyrics(limit)
        records = await self._collect_tracks_lyrics(tracks_without_lyrics)
        logger.info("Inserting lyrics records to db")

        await insert_records(engine=self._db_engine, records=records)

    async def _query_tracks_without_lyrics(self, limit: Optional[int]) -> List[Row]:
        logger.info("Querying database for tracks without lyrics")
        non_null_condition = or_(
            TrackIDMapping.genius_id.isnot(None),
            TrackIDMapping.shazam_id.isnot(None),
            TrackIDMapping.musixmatch_id.isnot(None),
        )
        track_lyrics_subquery = (
            select(TrackLyrics.id)
            .where(TrackLyrics.lyrics.isnot(None))
            .order_by(TrackLyrics.update_date.asc())
        )
        query = (
            select(TrackIDMapping.id, TrackIDMapping.genius_id, TrackIDMapping.musixmatch_id, TrackIDMapping.shazam_id)
            .where(TrackIDMapping.id.notin_(track_lyrics_subquery))
            .where(non_null_condition)
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.all()

    async def _collect_tracks_lyrics(self, tracks_without_lyrics: List[Row]) -> List[TrackLyrics]:
        records = []

        for source_details in self._prioritized_sources:
            source_records = await self._collect_single_source_records(tracks_without_lyrics, source_details)
            records.extend(source_records)
            self._remove_found_tracks_from_missing_ids(
                tracks_without_lyrics=tracks_without_lyrics,
                source_records=source_records
            )

        missing_lyrics_records = [TrackLyrics(id=row.id) for row in tracks_without_lyrics]
        return records + missing_lyrics_records

    async def _collect_single_source_records(self,
                                             tracks_without_lyrics: List[Row],
                                             source_details: LyricsSourceDetails) -> List[TrackLyrics]:
        logger.info(f"Collecting lyrics from `{source_details.data_source.value}` source")
        non_missing_ids = self._extract_non_missing_collector_ids(tracks_without_lyrics, source_details.column)
        ids_lyrics_mapping = await source_details.collector.collect(non_missing_ids)

        return self._records_serializer.serialize(
            ids_lyrics_mapping=ids_lyrics_mapping,
            source_details=source_details,
            track_ids_mapping=tracks_without_lyrics
        )

    @staticmethod
    def _extract_non_missing_collector_ids(tracks_without_lyrics: List[Row], column: TrackIDMapping) -> List[str]:
        non_missing_ids = set()

        for row in tracks_without_lyrics:
            row_id = getattr(row, column.key)

            if row_id is not None:
                non_missing_ids.add(row_id)

        return list(non_missing_ids)

    @staticmethod
    def _remove_found_tracks_from_missing_ids(tracks_without_lyrics: List[Row],
                                              source_records: List[TrackLyrics]) -> None:
        source_ids = [record.id for record in source_records]

        for i, row in enumerate(tracks_without_lyrics):
            if row.id in source_ids:
                tracks_without_lyrics.pop(i)
