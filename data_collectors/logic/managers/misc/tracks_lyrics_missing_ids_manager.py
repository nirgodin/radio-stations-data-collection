from typing import Optional, List

from genie_common.tools import logger
from genie_datastores.postgres.models import SpotifyTrack, TrackLyrics
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IManager
from data_collectors.logic.inserters.postgres import ChunksDatabaseInserter


class TracksLyricsMissingIDsManager(IManager):
    def __init__(self, db_engine: AsyncEngine, chunks_inserter: ChunksDatabaseInserter):
        self._db_engine = db_engine
        self._chunks_inserter = chunks_inserter

    async def run(self, limit: Optional[int]) -> None:
        logger.info("Starting to run TracksLyricsMissingIDsManager")
        missing_ids = await self._query_missing_ids(limit)

        if missing_ids:
            await self._insert_records(missing_ids)
        else:
            logger.info("Did not find any missing id. Skipping insertion")

    async def _query_missing_ids(self, limit: Optional[int]) -> List[str]:
        logger.info("Querying tracks ids that are missing from tracks_lyrics table")
        tracks_lyrics_subquery = select(TrackLyrics.id)
        query = select(SpotifyTrack.id).where(SpotifyTrack.id.notin_(tracks_lyrics_subquery)).limit(limit)
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().all()

    async def _insert_records(self, missing_ids: List[str]) -> None:
        records = [TrackLyrics(id=track_id) for track_id in missing_ids]
        logger.info("Inserting missing ids to tracks_lyrics_table")

        await self._chunks_inserter.insert(records)
