from genie_datastores.postgres.operations import get_database_engine
from spotipyio.auth import SpotifySession

from data_collectors.components.environment_component_factory import EnvironmentComponentFactory
from data_collectors.components.sessions_component_factory import SessionsComponentFactory
from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.exporters import *


class ExportersComponentFactory:
    def __init__(self,
                 env: EnvironmentComponentFactory = EnvironmentComponentFactory(),
                 sessions: SessionsComponentFactory = SessionsComponentFactory(),
                 tools: ToolsComponentFactory = ToolsComponentFactory()):
        self.env = env
        self.sessions = sessions
        self.tools = tools

    def get_chart_entries_mistakes_exporter(self) -> ChartsEntriesMistakesExporter:
        return ChartsEntriesMistakesExporter(
            db_engine=get_database_engine(),
            sheets_uploader=self.tools.get_google_sheets_uploader()
        )

    def get_shazam_matches_exporter(self) -> ShazamMatchesExporter:
        return ShazamMatchesExporter(
            db_engine=get_database_engine(),
            sheets_uploader=self.tools.get_google_sheets_uploader()
        )

    def get_users_playlists_exporter(self, spotify_session: SpotifySession) -> UsersPlaylistsExporter:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        return UsersPlaylistsExporter(
            spotify_client=spotify_client,
            google_sheets_uploader=self.tools.get_google_sheets_uploader()
        )
