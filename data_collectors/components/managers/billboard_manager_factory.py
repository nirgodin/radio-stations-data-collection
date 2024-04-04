from aiohttp import ClientSession
from genie_datastores.postgres.operations import get_database_engine
from spotipyio.logic.authentication.spotify_session import SpotifySession

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *


class BillboardManagerFactory(BaseManagerFactory):
    def get_billboard_manager(self, spotify_session: SpotifySession, client_session: ClientSession) -> BillboardManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        chunks_inserter = self.inserters.get_chunks_database_inserter()

        return BillboardManager(
            db_engine=get_database_engine(),
            charts_collector=self.collectors.billboard.get_charts_collector(client_session),
            tracks_collector=self.collectors.billboard.get_tracks_collector(client_session, spotify_client),
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            tracks_inserter=self.inserters.billboard.get_tracks_inserter(),
            charts_inserter=self.inserters.billboard.get_charts_inserter(chunks_inserter),
            tracks_updater=self.updaters.get_billboard_tracks_updater()
        )
