from aiohttp import ClientSession
from genie_datastores.mongo.operations import initialize_mongo
from genie_datastores.postgres.operations import get_database_engine
from spotipyio.tools.matching import EntityMatcher

from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.collectors import (
    GeniusSearchCollector,
    GeniusLyricsCollector,
    GeniusTracksCollector,
    GeniusArtistsCollector,
    GeniusArtistsExistingDetailsCollector,
)
from data_collectors.tools import (
    GeniusTrackEntityExtractor,
    GeniusArtistEntityExtractor,
)


class GeniusCollectorsComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self._tools = tools

    def get_search_collector(self, session: ClientSession) -> GeniusSearchCollector:
        entity_matcher = EntityMatcher({GeniusTrackEntityExtractor(): 0.7, GeniusArtistEntityExtractor(): 0.3})
        return GeniusSearchCollector(
            session=session,
            pool_executor=self._tools.get_pool_executor(),
            entity_matcher=self._tools.get_multi_entity_matcher(entity_matcher),
        )

    def get_lyrics_collector(self, session: ClientSession) -> GeniusLyricsCollector:
        return GeniusLyricsCollector(session=session, pool_executor=self._tools.get_pool_executor())

    def get_tracks_collector(self, session: ClientSession) -> GeniusTracksCollector:
        return GeniusTracksCollector(session=session, pool_executor=self._tools.get_pool_executor())

    def get_artists_collector(self, session: ClientSession) -> GeniusArtistsCollector:
        return GeniusArtistsCollector(session=session, pool_executor=self._tools.get_pool_executor())

    async def get_artists_existing_details_collector(
        self,
    ) -> GeniusArtistsExistingDetailsCollector:
        await initialize_mongo()
        return GeniusArtistsExistingDetailsCollector(
            db_engine=get_database_engine(),
            pool_executor=self._tools.get_pool_executor(),
        )
