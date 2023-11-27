from aiohttp import ClientSession
from postgres_client import get_database_engine
from shazamio import Shazam
from spotipyio.logic.authentication.spotify_session import SpotifySession

from data_collectors.components.collectors import CollectorsComponentFactory
from data_collectors.components.environment_component_factory import EnvironmentComponentFactory
from data_collectors.components.inserters import InsertersComponentFactory
from data_collectors.components.sessions_component_factory import SessionsComponentFactory
from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.components.updaters import UpdatersComponentFactory

from data_collectors.logic.inserters.shazam_insertions_manager import ShazamInsertionsManager
from data_collectors.logic.managers import *
from data_collectors.tools import AioPoolExecutor


class ComponentFactory:
    def __init__(self,
                 collectors: CollectorsComponentFactory = CollectorsComponentFactory(),
                 inserters: InsertersComponentFactory = InsertersComponentFactory(),
                 updaters: UpdatersComponentFactory = UpdatersComponentFactory(),
                 env: EnvironmentComponentFactory = EnvironmentComponentFactory(),
                 sessions: SessionsComponentFactory = SessionsComponentFactory(),
                 tools: ToolsComponentFactory = ToolsComponentFactory()):
        self.collectors = collectors
        self.inserters = inserters
        self.updaters = updaters
        self.env = env
        self.sessions = sessions
        self.tools = tools

    def get_musixmatch_missing_ids_manager(self, session: ClientSession) -> MusixmatchMissingIDsManager:
        pool_executor = self.tools.get_pool_executor()
        api_key = self.env.get_musixmatch_api_key()

        return MusixmatchMissingIDsManager(
            db_engine=get_database_engine(),
            search_collector=self.collectors.musixmatch.get_search_collector(session, pool_executor, api_key),
            track_ids_updater=self.updaters.get_track_ids_updater(),
        )

    def get_shazam_top_tracks_manager(self) -> ShazamTopTracksManager:
        shazam = self.tools.get_shazam()
        pool_executor = self.tools.get_pool_executor()

        return ShazamTopTracksManager(
            top_tracks_collector=self.collectors.shazam.get_top_tracks_collector(shazam, pool_executor),
            insertions_manager=self.get_insertions_manager(shazam, pool_executor),
            top_tracks_inserter=self.inserters.shazam.get_top_tracks_inserter()
        )

    def get_shazam_missing_ids_manager(self) -> ShazamMissingIDsManager:
        shazam = self.tools.get_shazam()
        pool_executor = self.tools.get_pool_executor()

        return ShazamMissingIDsManager(
            db_engine=get_database_engine(),
            search_collector=self.collectors.shazam.get_search_collector(shazam, pool_executor),
            insertions_manager=self.get_insertions_manager(shazam, pool_executor),
            track_ids_updater=self.updaters.get_track_ids_updater(),
        )

    def get_insertions_manager(self, shazam: Shazam, pool_executor: AioPoolExecutor) -> ShazamInsertionsManager:
        return ShazamInsertionsManager(
            artists_collector=self.collectors.shazam.get_artists_collector(shazam, pool_executor),
            tracks_collector=self.collectors.shazam.get_tracks_collector(shazam, pool_executor),
            artists_inserter=self.inserters.shazam.get_artists_inserter(),
            tracks_inserter=self.inserters.shazam.get_tracks_inserter()
        )

    def get_billboard_manager(self, spotify_session: SpotifySession, client_session: ClientSession) -> BillboardManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        return BillboardManager(
            db_engine=get_database_engine(),
            charts_collector=self.collectors.billboard.get_charts_collector(client_session),
            tracks_collector=self.collectors.billboard.get_tracks_collector(client_session, spotify_client),
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            tracks_inserter=self.inserters.billboard.get_tracks_inserter(),
            charts_inserter=self.inserters.billboard.get_charts_inserter(),
            tracks_updater=self.updaters.get_billboard_tracks_updater()
        )

    def get_radio_snapshots_manager(self, spotify_session: SpotifySession) -> RadioStationsSnapshotsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        return RadioStationsSnapshotsManager(
            spotify_client=spotify_client,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            radio_tracks_database_inserter=self.inserters.get_radio_tracks_inserter()
        )
