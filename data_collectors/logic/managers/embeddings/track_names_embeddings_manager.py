from typing import Optional, List

from genie_common.tools import logger
from genie_datastores.postgres.models import SpotifyTrack, SpotifyArtist, Track
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.musixmatch_consts import ARTIST_NAME
from data_collectors.contract import IManager
from data_collectors.logic.collectors import TrackNamesEmbeddingsCollector
from data_collectors.logic.models import MissingTrack, DBUpdateRequest
from data_collectors.logic.updaters import ValuesDatabaseUpdater


class TrackNamesEmbeddingsManager(IManager):
    def __init__(
        self,
        db_engine: AsyncEngine,
        embeddings_collector: TrackNamesEmbeddingsCollector,
        db_updater: ValuesDatabaseUpdater,
    ):
        self._embeddings_collector = embeddings_collector
        self._db_updater = db_updater
        self._db_engine = db_engine

    async def run(self, limit: Optional[int]) -> None:
        logger.info("Starting to run track names embeddings manager")
        missing_tracks = await self._collect_missing_embeddings_tracks(limit)
        batch_id = await self._embeddings_collector.collect(missing_tracks)
        await self._update_postgres_embeddings_exist(missing_tracks, batch_id)

    async def _collect_missing_embeddings_tracks(self, limit: Optional[int]) -> List[MissingTrack]:
        logger.info("Querying tracks without name embeddings")
        query = (
            select(
                Track.id,
                SpotifyTrack.id,
                SpotifyTrack.name,
                SpotifyArtist.name.label(ARTIST_NAME),
            )
            .where(Track.id == SpotifyTrack.id)
            .where(SpotifyTrack.artist_id == SpotifyArtist.id)
            .where(Track.has_name_embeddings.is_(False))
            .where(Track.batch_id.is_(None))
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return [MissingTrack.from_row(row) for row in query_result.all()]

    async def _update_postgres_embeddings_exist(self, missing_tracks: List[MissingTrack], batch_id: str) -> None:
        logger.info("Starting to update Postgres database of new embeddings batch id")
        update_requests = []

        for missing_track in missing_tracks:
            request = DBUpdateRequest(id=missing_track.spotify_id, values={Track.batch_id: batch_id})
            update_requests.append(request)

        await self._db_updater.update(update_requests)
