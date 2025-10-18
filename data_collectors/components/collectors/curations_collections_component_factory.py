from aiohttp import ClientSession
from spotipyio import SpotifyClient

from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.collectors import JosieCurationsCollector, SpotifyPlaylistsCurationsCollector


class CurationsCollectorsComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self._tools = tools

    def get_josie_collector(self, session: ClientSession) -> JosieCurationsCollector:
        return JosieCurationsCollector(
            josie_client=self._tools.get_josie_client(session),
            db_engine=self._tools.get_database_engine(),
        )

    @staticmethod
    def get_spotify_playlists_collector(spotify_client: SpotifyClient) -> SpotifyPlaylistsCurationsCollector:
        return SpotifyPlaylistsCurationsCollector(spotify_client)
