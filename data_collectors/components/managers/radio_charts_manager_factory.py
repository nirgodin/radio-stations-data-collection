from genie_datastores.postgres.operations import get_database_engine
from spotipyio.logic.authentication.spotify_session import SpotifySession

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *


class RadioChartsManagerFactory(BaseManagerFactory):
    def get_radio_charts_manager(self, spotify_session: SpotifySession) -> RadioChartsManager:
        drive_client = self.tools.get_google_drive_client()
        spotify_client = self.tools.get_spotify_client(spotify_session)
        tracks_collector = self.collectors.radio_charts.get_tracks_collector(
            pool_executor=self.tools.get_pool_executor(),
            spotify_client=spotify_client
        )

        return RadioChartsManager(
            db_engine=get_database_engine(),
            drive_client=drive_client,
            charts_data_collector=self.collectors.radio_charts.get_charts_collector(drive_client),
            charts_tracks_collector=tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            chart_entries_inserter=self.inserters.get_chart_entries_inserter()
        )

    def get_glglz_charts_manager(self, spotify_session: SpotifySession) -> GlglzChartsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        tracks_collector = self.collectors.radio_charts.get_tracks_collector(
            pool_executor=self.tools.get_pool_executor(),
            spotify_client=spotify_client
        )

        return GlglzChartsManager(
            chart_entries_inserter=self.inserters.get_chart_entries_inserter(),
            charts_data_collector=self.collectors.radio_charts.get_glglz_charts_collector(),
            charts_tracks_collector=tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client)
        )

    def get_spotify_charts_manager(self, spotify_session: SpotifySession) -> SpotifyChartsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        tracks_collector = self.collectors.radio_charts.get_tracks_collector(
            pool_executor=self.tools.get_pool_executor(),
            spotify_client=spotify_client
        )

        return SpotifyChartsManager(
            chart_entries_inserter=self.inserters.get_chart_entries_inserter(),
            charts_data_collector=self.collectors.radio_charts.get_spotify_charts_collector(spotify_client),
            charts_tracks_collector=tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client)
        )
