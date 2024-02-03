from typing import Optional, List, Dict

from genie_common.tools import logger
from genie_datastores.milvus import MilvusClient
from genie_datastores.postgres.models import SpotifyTrack, SpotifyArtist, Track
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.updaters import ValuesDatabaseUpdater
from data_collectors.consts.milvus_consts import EMBEDDINGS, TRACK_NAMES_EMBEDDINGS_COLLECTION
from data_collectors.consts.musixmatch_consts import ARTIST_NAME
from data_collectors.consts.spotify_consts import ID, NAME
from data_collectors.logic.collectors import TrackNamesEmbeddingsCollector
from data_collectors.contract import IManager
from data_collectors.logic.models import MissingTrack, DBUpdateRequest


class TrackNamesEmbeddingsManager(IManager):
    def __init__(self,
                 db_engine: AsyncEngine,
                 embeddings_collector: TrackNamesEmbeddingsCollector,
                 milvus_client: MilvusClient,
                 db_updater: ValuesDatabaseUpdater):
        self._embeddings_collector = embeddings_collector
        self._milvus_client = milvus_client
        self._db_updater = db_updater
        self._db_engine = db_engine

    async def run(self, limit: Optional[int]) -> None:
        logger.info("Starting to run track names embeddings manager")
        missing_tracks = await self._collect_missing_embeddings_tracks(limit)
        tracks_embeddings_mapping = await self._embeddings_collector.collect(missing_tracks)
        valid_embeddings_mapping = {k: v for k, v in tracks_embeddings_mapping.items() if v is not None}
        await self._insert_embeddings_records(valid_embeddings_mapping)
        await self._update_postgres_embeddings_exist(valid_embeddings_mapping)

    async def _collect_missing_embeddings_tracks(self, limit: Optional[int]) -> List[MissingTrack]:
        logger.info("Querying tracks without name embeddings")
        query = (
            select(Track.id, SpotifyTrack.id, SpotifyTrack.name, SpotifyArtist.name.label(ARTIST_NAME))
            .where(Track.id == SpotifyTrack.id)
            .where(SpotifyTrack.artist_id == SpotifyArtist.id)
            .where(Track.has_name_embeddings.is_(False))
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return [MissingTrack.from_row(row) for row in query_result.all()]

    async def _insert_embeddings_records(self, mapping: Dict[MissingTrack, Optional[List[float]]]) -> None:
        records = self._convert_mapping_to_records(mapping)
        logger.info(f"Starting to insert name embeddings for {len(records)} tracks")
        await self._milvus_client.insert(
            collection_name=TRACK_NAMES_EMBEDDINGS_COLLECTION,
            records=records
        )

        logger.info(f"Successfully inserted tracks name embeddings to Milvus vector database")

    @staticmethod
    def _convert_mapping_to_records(mapping: Dict[MissingTrack, List[float]]) -> List[dict]:
        logger.info("Converting missing tracks to embeddings mapping to data records")
        records = []

        for track, embeddings in mapping.items():
            record = {
                ID: track.spotify_id,
                NAME: track.track_name,
                EMBEDDINGS: embeddings
            }
            records.append(record)

        return records

    async def _update_postgres_embeddings_exist(self, mapping: Dict[MissingTrack, List[float]]) -> None:
        logger.info("Starting to update Postgres database of new existing embeddings")
        update_requests = []

        for missing_track, embeddings in mapping.items():
            request = DBUpdateRequest(
                id=missing_track.spotify_id,
                values={Track.has_name_embeddings: True}
            )
            update_requests.append(request)

        await self._db_updater.update(update_requests)
