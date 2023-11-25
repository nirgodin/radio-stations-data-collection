from aiohttp import ClientSession

from data_collectors.logic.collectors.musixmatch.musixmatch_search_collector import MusixmatchSearchCollector
from data_collectors.tools import AioPoolExecutor


class MusixmatchCollectorsComponentFactory:
    @staticmethod
    def get_search_collector(session: ClientSession, pool_executor: AioPoolExecutor, api_key: str):
        return MusixmatchSearchCollector(
            session=session,
            pool_executor=pool_executor,
            api_key=api_key
        )
