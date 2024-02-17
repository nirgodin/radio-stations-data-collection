from aiohttp import ClientSession

from data_collectors.logic.collectors import GeniusSearchCollector, GeniusLyricsCollector
from genie_common.tools import AioPoolExecutor


class GeniusCollectorsComponentFactory:
    @staticmethod
    def get_search_collector(session: ClientSession, pool_executor: AioPoolExecutor) -> GeniusSearchCollector:
        return GeniusSearchCollector(session=session, pool_executor=pool_executor)

    @staticmethod
    def get_lyrics_collector(session: ClientSession, pool_executor: AioPoolExecutor) -> GeniusLyricsCollector:
        return GeniusLyricsCollector(session=session, pool_executor=pool_executor)
