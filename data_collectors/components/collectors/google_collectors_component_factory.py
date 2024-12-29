from aiohttp import ClientSession
from genie_datastores.postgres.operations import get_database_engine

from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.collectors import (
    GoogleGeocodingCollector,
    GeminiArtistsAboutParsingCollector,
)


class GoogleCollectorsComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self._tools = tools

    def get_geocoding_collector(
        self, session: ClientSession
    ) -> GoogleGeocodingCollector:
        return GoogleGeocodingCollector(
            db_engine=get_database_engine(),
            pool_executor=self._tools.get_pool_executor(),
            session=session,
        )

    def get_artists_about_parsing_collector(self) -> GeminiArtistsAboutParsingCollector:
        return GeminiArtistsAboutParsingCollector(
            pool_executor=self._tools.get_pool_executor(),
            model=self._tools.get_gemini_model(),
        )
