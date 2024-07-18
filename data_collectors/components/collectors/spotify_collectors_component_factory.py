from aiohttp import ClientSession
from genie_datastores.postgres.operations import get_database_engine
from spotipyio import SpotifyClient

from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.collectors import SpotifyArtistsImagesCollector, SpotifyArtistsExistingDetailsCollector, \
    SpotifyArtistsAboutCollector


class SpotifyCollectorsComponentFactory:
    def __init__(self, tools: ToolsComponentFactory = ToolsComponentFactory()):
        self._tools = tools

    def get_artists_images_collector(self,
                                     client_session: ClientSession,
                                     spotify_client: SpotifyClient) -> SpotifyArtistsImagesCollector:
        return SpotifyArtistsImagesCollector(
            client_session=client_session,
            spotify_client=spotify_client,
            pool_executor=self._tools.get_pool_executor()
        )

    @staticmethod
    def get_artist_existing_details_collector() -> SpotifyArtistsExistingDetailsCollector:
        return SpotifyArtistsExistingDetailsCollector(get_database_engine())

    def get_spotify_artists_about_collector(self) -> SpotifyArtistsAboutCollector:
        return SpotifyArtistsAboutCollector(self._tools.get_pool_executor())
