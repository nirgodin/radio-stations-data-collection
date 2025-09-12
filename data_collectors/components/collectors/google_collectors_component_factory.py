from aiohttp import ClientSession

from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.collectors import (
    GoogleGeocodingCollector,
    GeminiArtistsAboutParsingCollector,
    GoogleArtistsWebPagesCollector,
)
from data_collectors.logic.models import Domain


class GoogleCollectorsComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self._tools = tools

    def get_geocoding_collector(self, session: ClientSession) -> GoogleGeocodingCollector:
        return GoogleGeocodingCollector(
            db_engine=self._tools.get_database_engine(),
            pool_executor=self._tools.get_pool_executor(),
            rapid_client=self._tools.get_rapid_api_client(session),
        )

    def get_artists_about_parsing_collector(self) -> GeminiArtistsAboutParsingCollector:
        return GeminiArtistsAboutParsingCollector(
            pool_executor=self._tools.get_pool_executor(),
            model=self._tools.get_gemini_model(),
        )

    def get_artists_web_pages_collector(self, session: ClientSession) -> GoogleArtistsWebPagesCollector:
        return GoogleArtistsWebPagesCollector(
            google_search_client=self._tools.get_google_search_client(session),
            pool_executor=self._tools.get_pool_executor(),
            domains=list(Domain),
        )
