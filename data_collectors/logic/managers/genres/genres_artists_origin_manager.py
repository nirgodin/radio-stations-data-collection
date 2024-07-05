from typing import Optional, List, Tuple

from genie_common.tools import logger
from genie_datastores.postgres.models import Artist, SpotifyArtist, Decision, Table
from genie_datastores.models import DataSource
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IManager
from data_collectors.logic.inserters.postgres import ChunksDatabaseInserter
from data_collectors.logic.models import DBUpdateRequest
from data_collectors.logic.updaters import ValuesDatabaseUpdater

GENRES_MAPPING = {
    "atl hip hop": "Atlanta, USA",
    "chicago rap": "Chicago, Illinois, USA",
    "chicago soul": "Chicago, Illinois, USA",
    "toronto rap": "Toronto, Ontario, Canada",
}


class GenresArtistsOriginManager(IManager):
    def __init__(self, db_engine: AsyncEngine, db_updater: ValuesDatabaseUpdater, db_inserter: ChunksDatabaseInserter):
        self._db_engine = db_engine
        self._db_updater = db_updater
        self._db_inserter = db_inserter

    async def run(self, limit: Optional[int]) -> None:
        logger.info("Starting to run GenresArtistsOriginManager")
        missing_origin_artists = await self._query_missing_origin_artists(limit)
        genres_to_update_requests = self._to_update_requests(missing_origin_artists)
        update_requests = [update_request for genre, update_request in genres_to_update_requests]
        await self._db_updater.update(update_requests)
        decision_records = self._to_decisions(genres_to_update_requests)
        await self._db_inserter.insert(decision_records)

    async def _query_missing_origin_artists(self, limit: Optional[int]) -> List[Row]:
        logger.info("Querying database for artists with genres indicating origin")
        origin_indicating_genres = list(GENRES_MAPPING.keys())
        query = (
            select(Artist.id, SpotifyArtist.genres)
            .where(Artist.id == SpotifyArtist.id)
            .where(Artist.origin.is_(None))
            .where(SpotifyArtist.genres.contains(origin_indicating_genres))
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.all()

    def _to_update_requests(self, rows: List[Row]) -> List[Tuple[str, DBUpdateRequest]]:
        logger.info("Transforming rows to update requests")
        update_requests = []

        for row in rows:
            genre, artist_origin = self._determine_single_artist_origin(row)
            request = DBUpdateRequest(
                id=row.id,
                values={Artist.origin: artist_origin}
            )
            update_requests.append((genre, request))

        return update_requests

    @staticmethod
    def _determine_single_artist_origin(row: Row) -> Tuple[str, str]:
        for genre, origin in GENRES_MAPPING.items():
            if genre in row.genres:
                return genre, origin

    @staticmethod
    def _to_decisions(genres_to_update_requests: List[Tuple[str, DBUpdateRequest]]) -> List[Decision]:
        logger.info("Transforming update requests to decisions entries")
        decisions = []

        for genre, request in genres_to_update_requests:
            decision = Decision(
                column=Artist.origin.key,
                source=DataSource.SPOTIFY,
                table=Table.ARTISTS,
                table_id=request.id,
                evidence=f"genre: {genre}"
            )
            decisions.append(decision)

        return decisions
