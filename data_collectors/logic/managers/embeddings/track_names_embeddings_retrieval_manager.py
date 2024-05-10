import json
from typing import List, Dict

from genie_common.tools import logger
from genie_common.utils import safe_nested_get
from genie_datastores.milvus import MilvusClient
from genie_datastores.postgres.models import Track, SpotifyTrack
from genie_datastores.postgres.operations import execute_query
from openai import OpenAI
from openai.types import Batch
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.milvus_consts import TRACK_NAMES_EMBEDDINGS_COLLECTION, EMBEDDINGS
from data_collectors.consts.spotify_consts import ID, NAME
from data_collectors.logic.models import DBUpdateRequest
from data_collectors.logic.updaters import ValuesDatabaseUpdater
from data_collectors.contract import IManager


class TrackNamesEmbeddingsRetrievalManager(IManager):
    def __init__(self,
                 db_engine: AsyncEngine,
                 openai: OpenAI,
                 milvus_client: MilvusClient,
                 db_updater: ValuesDatabaseUpdater):
        self._db_engine = db_engine
        self._openai = openai
        self._milvus_client = milvus_client
        self._db_updater = db_updater

    async def run(self):
        batches_ids = await self._query_missing_batches_ids()

        for batch_id in batches_ids:
            await self._handle_single_batch(batch_id)

    async def _query_missing_batches_ids(self) -> List[str]:
        logger.info("Querying database for unprocessed batches ids")
        query = (
            select(Track.batch_id)
            .distinct(Track.batch_id)
            .where(Track.has_name_embeddings.is_(False))
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().all()

    async def _handle_single_batch(self, batch_id: str) -> None:
        logger.info(f"Handling batch `{batch_id}`")
        batch = self._openai.batches.retrieve(batch_id)

        if batch.status == "completed":
            await self._upload_batch_embeddings(batch)
        else:
            logger.info(f"Batch status is `{batch.status}` and not completed. Skipping")

    async def _upload_batch_embeddings(self, batch: Batch) -> None:
        batch_file = self._openai.files.content(batch.output_file_id)
        data = batch_file.content.decode(encoding="utf-8")
        batch_records = self._to_batch_records(data)
        track_id_name_mapping = await self._query_tracks_names(batch.id)
        await self._insert_embeddings_records(batch_records, track_id_name_mapping)
        await self._update_postgres_embeddings_exist(batch_records)

    async def _query_tracks_names(self, batch_id: str) -> Dict[str, str]:
        query = (
            select(SpotifyTrack.id, SpotifyTrack.name)
            .where(SpotifyTrack.id == Track.id)
            .where(Track.batch_id == batch_id)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)
        tracks = query_result.all()

        return {track.id: track.name for track in tracks}

    @staticmethod
    def _to_batch_records(data: str) -> List[dict]:
        records = []

        for line in data.split("\n"):
            record = None

            try:
                record = json.loads(line)
            except:
                logger.warning(f"Failed to decode line `{line}`")

            if record is not None:
                records.append(record)

        return records

    async def _insert_embeddings_records(self, batch_records: List[dict], track_id_name_mapping: Dict[str, str]) -> None:
        records = self._to_milvus_records(batch_records, track_id_name_mapping)
        logger.info(f"Starting to insert name embeddings for {len(records)} tracks")
        await self._milvus_client.vectors.insert(
            collection_name=TRACK_NAMES_EMBEDDINGS_COLLECTION,
            records=records
        )

        logger.info(f"Successfully inserted tracks name embeddings to Milvus vector database")

    def _to_milvus_records(self, batch_records: List[dict], track_id_name_mapping: Dict[str, str]) -> List[dict]:
        logger.info("Converting batch records to embeddings data records")
        records = []

        for batch_record in batch_records:
            track_id = batch_record["custom_id"]
            record = {
                ID: track_id,
                NAME: track_id_name_mapping[track_id],
                EMBEDDINGS: self._extract_track_embeddings(batch_record)
            }
            records.append(record)

        return records

    @staticmethod
    def _extract_track_embeddings(record: dict) -> List[float]:
        data = safe_nested_get(record, ["response", "body", "data"])

        if data:
            first_record = data[0]
            return first_record["embedding"]

    async def _update_postgres_embeddings_exist(self, batch_records: List[dict]) -> None:
        logger.info("Starting to update Postgres database of new existing embeddings")
        update_requests = []

        for record in batch_records:
            request = DBUpdateRequest(
                id=record["custom_id"],
                values={Track.has_name_embeddings: True}
            )
            update_requests.append(request)

        await self._db_updater.update(update_requests)
