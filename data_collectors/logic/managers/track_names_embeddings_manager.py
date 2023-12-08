from typing import Optional, List

from genie_datastores.postgres.models import SpotifyTrack, SpotifyArtist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.musixmatch_consts import ARTIST_NAME
from data_collectors.logic.collectors import TrackNamesEmbeddingsCollector
from data_collectors.contract import IManager
from data_collectors.logic.models import MissingTrack


class TrackNamesEmbeddingsManager(IManager):
    def __init__(self,
                 db_engine: AsyncEngine,
                 embeddings_collector: TrackNamesEmbeddingsCollector,
                 embeddings_inserter: TrackNamesEmbeddingsInserter):
        self._embeddings_collector = embeddings_collector
        self._db_engine = db_engine

    async def run(self, limit: Optional[int]):
        missing_tracks = await self._collect_missing_embeddings_tracks(limit)
        ids_to_embeddings = await self._embeddings_collector.collect(missing_tracks)


    async def _collect_missing_embeddings_tracks(self, limit: Optional[int]) -> List[MissingTrack]:
        query = (
            select(SpotifyTrack.id, SpotifyTrack.name, SpotifyArtist.name.label(ARTIST_NAME))
            .where(SpotifyTrack.artist_id == SpotifyArtist.id)
            .where(SpotifyTrack.has_name_embeddings.is_(False))
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return [MissingTrack.from_row(row) for row in query_result.all()]
