from aiohttp import ClientSession
from genie_common.tools import AioPoolExecutor
from spotipyio.tools.matching import EntityMatcher

from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.collectors.musixmatch import (
    MusixmatchSearchCollector,
    MusixmatchLyricsCollector,
)
from data_collectors.tools import (
    MusixmatchTrackEntityExtractor,
    MusixmatchArtistEntityExtractor,
)


class MusixmatchCollectorsComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self._tools = tools

    def get_search_collector(
        self, session: ClientSession, pool_executor: AioPoolExecutor, api_key: str
    ) -> MusixmatchSearchCollector:
        entity_matcher = EntityMatcher(
            {
                MusixmatchTrackEntityExtractor(): 0.7,
                MusixmatchArtistEntityExtractor(): 0.3,
            }
        )
        return MusixmatchSearchCollector(
            session=session,
            pool_executor=pool_executor,
            api_key=api_key,
            entity_matcher=self._tools.get_multi_entity_matcher(entity_matcher),
        )

    @staticmethod
    def get_lyrics_collector(
        session: ClientSession, pool_executor: AioPoolExecutor, api_key: str
    ) -> MusixmatchLyricsCollector:
        return MusixmatchLyricsCollector(
            session=session, pool_executor=pool_executor, api_key=api_key
        )
