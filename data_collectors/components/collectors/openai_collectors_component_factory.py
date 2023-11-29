from aiohttp import ClientSession
from genie_common.openai import OpenAIClient
from postgres_client import get_database_engine

from data_collectors.logic.collectors import TrackNamesEmbeddingsCollector
from data_collectors.tools import AioPoolExecutor


class OpenAICollectorsComponentFactory:
    @staticmethod
    def get_track_names_embeddings_collector(pool_executor: AioPoolExecutor,
                                             session: ClientSession) -> TrackNamesEmbeddingsCollector:
        return TrackNamesEmbeddingsCollector(
            db_engine=get_database_engine(),
            pool_executor=pool_executor,
            openai_client=OpenAIClient.create(session)
        )
