from typing import List, Dict

from genie_common.tools import logger
from genie_datastores.milvus import MilvusClient
from genie_datastores.postgres.models import Track, SpotifyTrack
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.milvus_consts import TRACK_NAMES_EMBEDDINGS_COLLECTION
from data_collectors.consts.spotify_consts import ID
from data_collectors.contract import IManager
from data_collectors.logic.collectors import TrackNamesEmbeddingsRetrievalCollector
from data_collectors.logic.models import DBUpdateRequest
from data_collectors.logic.serializers import OpenAIBatchEmbeddingsSerializer
from data_collectors.logic.updaters import ValuesDatabaseUpdater


class TrackNamesEmbeddingsRetrievalManager(IManager):
    def __init__(
        self,
        db_engine: AsyncEngine,
        embeddings_retriever: TrackNamesEmbeddingsRetrievalCollector,
        milvus_client: MilvusClient,
        db_updater: ValuesDatabaseUpdater,
        embeddings_serializer: OpenAIBatchEmbeddingsSerializer = OpenAIBatchEmbeddingsSerializer(),
    ):
        self._db_engine = db_engine
        self._embeddings_retriever = embeddings_retriever
        self._milvus_client = milvus_client
        self._db_updater = db_updater
        self._embeddings_serializer = embeddings_serializer

    async def run(self):
        batches_ids = await self._query_missing_batches_ids()
        batches = await self._embeddings_retriever.collect(batches_ids)

        for batch_id, batch_records in batches.items():
            if batch_records:
                await self._handle_single_batch(batch_id, batch_records)

    async def _query_missing_batches_ids(self) -> List[str]:
        logger.info("Querying database for unprocessed batches ids")
        query = (
            select(Track.batch_id)
            .distinct(Track.batch_id)
            .where(Track.has_name_embeddings.is_(False))
            .where(Track.batch_id.isnot(None))
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().all()

    async def _handle_single_batch(
        self, batch_id: str, batch_records: List[dict]
    ) -> None:
        track_id_name_mapping = await self._query_tracks_names(batch_id)
        embeddings_records = self._embeddings_serializer.serialize(
            batch_records, track_id_name_mapping
        )
        await self._insert_embeddings_records(embeddings_records)
        await self._update_postgres_embeddings_exist(embeddings_records)

    async def _query_tracks_names(self, batch_id: str) -> Dict[str, str]:
        query = (
            select(SpotifyTrack.id, SpotifyTrack.name)
            .where(SpotifyTrack.id == Track.id)
            .where(Track.batch_id == batch_id)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)
        tracks = query_result.all()

        return {track.id: track.name for track in tracks}

    async def _insert_embeddings_records(self, records: List[dict]) -> None:
        logger.info(f"Starting to insert name embeddings for {len(records)} tracks")
        await self._milvus_client.vectors.insert(
            collection_name=TRACK_NAMES_EMBEDDINGS_COLLECTION, records=records
        )
        logger.info(
            "Successfully inserted tracks name embeddings to Milvus vector database"
        )

    async def _update_postgres_embeddings_exist(self, records: List[dict]) -> None:
        logger.info("Starting to update Postgres database of new existing embeddings")
        update_requests = []

        for record in records:
            request = DBUpdateRequest(
                id=record[ID], values={Track.has_name_embeddings: True}
            )
            update_requests.append(request)

        await self._db_updater.update(update_requests)
