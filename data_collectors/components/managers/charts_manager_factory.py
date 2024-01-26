from typing import Dict

from genie_datastores.postgres.models import Chart
from genie_datastores.postgres.operations import get_database_engine
from spotipyio.logic.authentication.spotify_session import SpotifySession

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *


class ChartsManagerFactory(BaseManagerFactory):
    def get_radio_charts_manager(self, spotify_session: SpotifySession) -> RadioChartsManager:
        drive_client = self.tools.get_google_drive_client()
        spotify_client = self.tools.get_spotify_client(spotify_session)
        pool_executor = self.tools.get_pool_executor()
        tracks_collector = self.collectors.charts.get_tracks_collector(
            pool_executor=pool_executor,
            spotify_client=spotify_client
        )
        chunks_generator = self.tools.get_chunks_generator(pool_executor)

        return RadioChartsManager(
            db_engine=get_database_engine(),
            drive_client=drive_client,
            charts_data_collector=self.collectors.charts.get_radio_charts_collector(drive_client),
            charts_tracks_collector=tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            chart_entries_inserter=self.inserters.get_chart_entries_inserter(chunks_generator)
        )

    def get_glglz_charts_manager(self, spotify_session: SpotifySession) -> GlglzChartsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        pool_executor = self.tools.get_pool_executor()
        tracks_collector = self.collectors.charts.get_tracks_collector(
            pool_executor=pool_executor,
            spotify_client=spotify_client
        )
        chunks_generator = self.tools.get_chunks_generator(pool_executor)

        return GlglzChartsManager(
            chart_entries_inserter=self.inserters.get_chart_entries_inserter(chunks_generator),
            charts_data_collector=self.collectors.charts.get_glglz_charts_collector(),
            charts_tracks_collector=tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            db_engine=get_database_engine()
        )

    def get_spotify_charts_manager(self, spotify_session: SpotifySession) -> PlaylistsChartsManager:
        playlist_id_to_chart_mapping = {
            '37i9dQZEVXbJ6IpvItkve3': Chart.SPOTIFY_DAILY_ISRAELI,
            '37i9dQZEVXbMDoHDwVN2tF': Chart.SPOTIFY_DAILY_INTERNATIONAL,
        }

        return self._get_playlists_chart_manager(
            spotify_session=spotify_session,
            playlist_id_to_chart_mapping=playlist_id_to_chart_mapping
        )

    def get_mako_hit_list_charts_manager(self, spotify_session: SpotifySession) -> PlaylistsChartsManager:
        playlist_id_to_chart_mapping = {
            '3Oh3oSaZjfsXcNwSpVMye2': Chart.MAKO_WEEKLY_HIT_LIST,
        }

        return self._get_playlists_chart_manager(
            spotify_session=spotify_session,
            playlist_id_to_chart_mapping=playlist_id_to_chart_mapping
        )

    def get_tagged_mistakes_manager(self) -> ChartsTaggedMistakesManager:
        pool_executor = self.tools.get_pool_executor()
        return ChartsTaggedMistakesManager(
            sheets_client=self.tools.get_google_sheets_client(),
            tagged_mistakes_collector=self.collectors.charts.get_charts_tagged_mistakes_collector(pool_executor)
        )

    def _get_playlists_chart_manager(self,
                                     spotify_session: SpotifySession,
                                     playlist_id_to_chart_mapping: Dict[str, Chart]) -> PlaylistsChartsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        pool_executor = self.tools.get_pool_executor()
        tracks_collector = self.collectors.charts.get_tracks_collector(
            pool_executor=pool_executor,
            spotify_client=spotify_client
        )
        data_collector = self.collectors.charts.get_playlists_charts_collector(
            spotify_client=spotify_client,
            playlist_id_to_chart_mapping=playlist_id_to_chart_mapping
        )
        chunks_generator = self.tools.get_chunks_generator(pool_executor)

        return PlaylistsChartsManager(
            chart_entries_inserter=self.inserters.get_chart_entries_inserter(chunks_generator),
            charts_data_collector=data_collector,
            charts_tracks_collector=tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client)
        )
