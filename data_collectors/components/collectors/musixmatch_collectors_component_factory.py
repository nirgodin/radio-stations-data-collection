from aiohttp import ClientSession

from data_collectors.logic.collectors.musixmatch import MusixmatchSearchCollector, MusixmatchLyricsCollector
from data_collectors.tools import AioPoolExecutor


class MusixmatchCollectorsComponentFactory:
    @staticmethod
    def get_search_collector(session: ClientSession,
                             pool_executor: AioPoolExecutor,
                             api_key: str) -> MusixmatchSearchCollector:
        return MusixmatchSearchCollector(
            session=session,
            pool_executor=pool_executor,
            api_key=api_key
        )

    @staticmethod
    def get_lyrics_collector(session: ClientSession,
                             pool_executor: AioPoolExecutor,
                             api_key: str) -> MusixmatchLyricsCollector:
        return MusixmatchLyricsCollector(
            session=session,
            pool_executor=pool_executor,
            api_key=api_key
        )
