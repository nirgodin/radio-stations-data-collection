from aiohttp import ClientSession

from data_collectors.logic.collectors import GeniusSearchCollector
from data_collectors.tools import AioPoolExecutor


class GeniusCollectorsComponentFactory:
    @staticmethod
    def get_search_collector(session: ClientSession, pool_executor: AioPoolExecutor) -> GeniusSearchCollector:
        return GeniusSearchCollector(session=session, pool_executor=pool_executor)
