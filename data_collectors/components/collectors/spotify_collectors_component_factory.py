from aiohttp import ClientSession
from genie_common.tools import AioPoolExecutor
from spotipyio import SpotifyClient

from data_collectors import SpotifyArtistsImagesCollector


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
