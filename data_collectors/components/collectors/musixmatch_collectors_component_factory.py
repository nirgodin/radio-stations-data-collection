from aiohttp import ClientSession
from spotipyio import EntityMatcher

from data_collectors.logic.collectors.musixmatch import MusixmatchSearchCollector, MusixmatchLyricsCollector
from genie_common.tools import AioPoolExecutor

from data_collectors.tools import MultiEntityMatcher, MusixmatchTrackEntityExtractor, MusixmatchArtistEntityExtractor


class MusixmatchCollectorsComponentFactory:
    @staticmethod
    def get_search_collector(session: ClientSession,
                             pool_executor: AioPoolExecutor,
                             api_key: str) -> MusixmatchSearchCollector:
        entity_matcher = EntityMatcher(
            {
                MusixmatchTrackEntityExtractor(): 0.7,
                MusixmatchArtistEntityExtractor(): 0.3
            }
        )
        return MusixmatchSearchCollector(
            session=session,
            pool_executor=pool_executor,
            api_key=api_key,
            entity_matcher=MultiEntityMatcher(entity_matcher)
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
