from aiohttp import ClientSession
from genie_datastores.mongo.operations import initialize_mongo
from genie_datastores.postgres.operations import get_database_engine
from spotipyio import SpotifyClient

from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.collectors import SpotifyArtistsImagesCollector, SpotifyArtistsExistingDetailsCollector, \
    SpotifyArtistsAboutCollector


class SpotifyCollectorsComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self._tools = tools

    def get_artists_images_collector(self,
                                     client_session: ClientSession,
                                     spotify_client: SpotifyClient) -> SpotifyArtistsImagesCollector:
        return SpotifyArtistsImagesCollector(
            client_session=client_session,
            spotify_client=spotify_client,
            pool_executor=self._tools.get_pool_executor()
        )

    async def get_artist_existing_details_collector(self) -> SpotifyArtistsExistingDetailsCollector:
        await initialize_mongo()
        return SpotifyArtistsExistingDetailsCollector(
            db_engine=get_database_engine(),
            pool_executor=self._tools.get_pool_executor()
        )

    def get_spotify_artists_about_collector(self) -> SpotifyArtistsAboutCollector:
        return SpotifyArtistsAboutCollector(self._tools.get_pool_executor())
