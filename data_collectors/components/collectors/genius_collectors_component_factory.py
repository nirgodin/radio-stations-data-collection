from aiohttp import ClientSession
from spotipyio import EntityMatcher

from data_collectors.logic.collectors import GeniusSearchCollector, GeniusLyricsCollector
from genie_common.tools import AioPoolExecutor

from data_collectors.tools import MultiEntityMatcher, GeniusTrackEntityExtractor, GeniusArtistEntityExtractor


class GeniusCollectorsComponentFactory:
    @staticmethod
    def get_search_collector(session: ClientSession, pool_executor: AioPoolExecutor) -> GeniusSearchCollector:
        entity_matcher = EntityMatcher(
            {
                GeniusTrackEntityExtractor(): 0.7,
                GeniusArtistEntityExtractor(): 0.3
            }
        )
        return GeniusSearchCollector(
            session=session,
            pool_executor=pool_executor,
            entity_matcher=MultiEntityMatcher(entity_matcher)
        )

    @staticmethod
    def get_lyrics_collector(session: ClientSession, pool_executor: AioPoolExecutor) -> GeniusLyricsCollector:
        return GeniusLyricsCollector(session=session, pool_executor=pool_executor)
