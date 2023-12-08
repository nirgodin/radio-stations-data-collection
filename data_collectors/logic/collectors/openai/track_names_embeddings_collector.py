from typing import List, Dict, Optional

from genie_common.models.openai import EmbeddingsModel
from genie_common.openai import OpenAIClient
from genie_common.tools import logger
from genie_datastores.postgres.models import SpotifyTrack
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import ICollector
from genie_common.utils import merge_dicts
from data_collectors.tools import AioPoolExecutor


class TrackNamesEmbeddingsCollector(ICollector):
    def __init__(self, db_engine: AsyncEngine, pool_executor: AioPoolExecutor, openai_client: OpenAIClient):
        self._db_engine = db_engine
        self._openai_client = openai_client
        self._pool_executor = pool_executor

    async def collect(self, ids: List[str], limit: Optional[int] = None) -> Dict[str, List[float]]:
        tracks_ids_and_names = await self._query_tracks_names(ids, limit)
        results = await self._pool_executor.run(
            iterable=tracks_ids_and_names,
            func=self._get_single_name_embeddings,
            expected_type=dict
        )

        return merge_dicts(*results)

    async def _query_tracks_names(self, ids: List[str], limit: Optional[int]) -> List[Row]:
        logger.info(f"Querying tracks names of {len(ids)} tracks ids")
        query = (
            select(SpotifyTrack.id, SpotifyTrack.name)
            .distinct(SpotifyTrack.id)
            .where(SpotifyTrack.id.in_(ids))
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.all()

    async def _get_single_name_embeddings(self, row: Row) -> Dict[str, Optional[List[float]]]:
        embeddings = await self._openai_client.embeddings.collect(text=row.name, model=EmbeddingsModel.ADA)
        return {row.id: embeddings}
