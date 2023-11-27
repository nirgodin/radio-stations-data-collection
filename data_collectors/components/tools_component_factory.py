from shazamio import Shazam
from spotipyio import SpotifyClient
from spotipyio.logic.authentication.spotify_session import SpotifySession

from data_collectors.tools import AioPoolExecutor


class ToolsComponentFactory:
    @staticmethod
    def get_pool_executor(pool_size: int = 5, validate_results: bool = True) -> AioPoolExecutor:
        return AioPoolExecutor(pool_size, validate_results)

    @staticmethod
    def get_shazam(language: str = "EN") -> Shazam:
        return Shazam(language)

    @staticmethod
    def get_spotify_client(spotify_session: SpotifySession) -> SpotifyClient:
        return SpotifyClient.create(spotify_session)
