from aiohttp import ClientSession
from genie_common.tools import AioPoolExecutor
from genie_datastores.postgres.operations import get_database_engine
from spotipyio import SpotifyClient

from data_collectors.logic.collectors import SpotifyArtistsImagesCollector, SpotifyArtistsExistingDetailsCollector


class SpotifyCollectorsComponentFactory:
    @staticmethod
    def get_artists_images_collector(client_session: ClientSession,
                                     spotify_client: SpotifyClient,
                                     pool_executor: AioPoolExecutor) -> SpotifyArtistsImagesCollector:
        return SpotifyArtistsImagesCollector(
            client_session=client_session,
            spotify_client=spotify_client,
            pool_executor=pool_executor
        )

    @staticmethod
    def get_artist_existing_details_collector() -> SpotifyArtistsExistingDetailsCollector:
        return SpotifyArtistsExistingDetailsCollector(get_database_engine())
