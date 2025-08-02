from aiohttp import ClientSession
from playwright.async_api import Browser
from spotipyio import SpotifyClient

from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.collectors import (
    SpotifyArtistsImagesCollector,
    SpotifyArtistsExistingDetailsCollector,
    SpotifyArtistsAboutCollector,
)


class SpotifyCollectorsComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self._tools = tools

    def get_artists_images_collector(
        self, client_session: ClientSession, spotify_client: SpotifyClient
    ) -> SpotifyArtistsImagesCollector:
        return SpotifyArtistsImagesCollector(
            client_session=client_session,
            spotify_client=spotify_client,
            pool_executor=self._tools.get_pool_executor(),
        )

    async def get_artist_existing_details_collector(
        self,
    ) -> SpotifyArtistsExistingDetailsCollector:
        return SpotifyArtistsExistingDetailsCollector(
            db_engine=self._tools.get_database_engine(),
            pool_executor=self._tools.get_pool_executor(),
        )

    def get_spotify_artists_about_collector(self, browser: Browser) -> SpotifyArtistsAboutCollector:
        return SpotifyArtistsAboutCollector(
            pool_executor=self._tools.get_pool_executor(),
            browser=browser,
        )
