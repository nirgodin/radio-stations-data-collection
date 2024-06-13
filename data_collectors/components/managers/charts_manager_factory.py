from typing import Dict

from aiohttp import ClientSession
from genie_datastores.postgres.models import Chart
from genie_datastores.postgres.operations import get_database_engine
from spotipyio import SpotifyClient, EntityMatcher, TrackEntityExtractor, TrackSearchResultArtistEntityExtractor
from spotipyio.logic.authentication.spotify_session import SpotifySession

from data_collectors.logic.collectors import ChartsTracksCollector, ArtistTranslatorChartKeySearcher
from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *


class ChartsManagerFactory(BaseManagerFactory):
    def get_radio_charts_manager(self, spotify_session: SpotifySession) -> RadioChartsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        tracks_collector = self.collectors.charts.get_tracks_collector(spotify_client)

        return self._get_radio_chart_manager(spotify_client, tracks_collector)

    def get_translated_artist_radio_charts_manager(self, spotify_session: SpotifySession) -> RadioChartsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        extractors = {TrackEntityExtractor(): 0.5, TrackSearchResultArtistEntityExtractor(): 0.5}
        entity_matcher = EntityMatcher(extractors=extractors, threshold=0.75)
        key_searcher = ArtistTranslatorChartKeySearcher(
            spotify_client=spotify_client,
            translation_adapter=self.tools.get_translation_adapter(),
            entity_matcher=entity_matcher
        )
        tracks_collector = self.collectors.charts.get_tracks_collector(
            spotify_client=spotify_client,
            key_searcher=key_searcher
        )

        return self._get_radio_chart_manager(spotify_client, tracks_collector)

    def get_eurovision_charts_manager(self,
                                      client_session: ClientSession,
                                      spotify_session: SpotifySession) -> EurovisionChartsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        tracks_collector = self.collectors.charts.get_tracks_collector(spotify_client)
        eurovision_charts_collector = self.collectors.charts.get_eurovision_charts_collector(client_session)

        return EurovisionChartsManager(
            db_engine=get_database_engine(),
            charts_data_collector=eurovision_charts_collector,
            charts_tracks_collector=tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            chart_entries_inserter=self.inserters.get_chart_entries_inserter()
        )

    def get_glglz_charts_manager(self, spotify_session: SpotifySession) -> GlglzChartsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        tracks_collector = self.collectors.charts.get_tracks_collector(spotify_client)

        return GlglzChartsManager(
            chart_entries_inserter=self.inserters.get_chart_entries_inserter(),
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

    def get_tagged_mistakes_manager(self, spotify_session: SpotifySession) -> ChartsTaggedMistakesManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        tagged_mistakes_tracks_collector = self.collectors.charts.get_tagged_mistakes_tracks_collector(spotify_client)

        return ChartsTaggedMistakesManager(
            sheets_client=self.tools.get_google_sheets_client(),
            tagged_mistakes_collector=self.collectors.charts.get_charts_tagged_mistakes_collector(),
            tagged_mistakes_tracks_collector=tagged_mistakes_tracks_collector,
            db_updater=self.updaters.get_values_updater(),
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client)
        )

    def get_israeli_artists_manager(self) -> ChartsIsraeliArtistsManager:
        return ChartsIsraeliArtistsManager(
            db_engine=get_database_engine(),
            db_updater=self.updaters.get_values_updater(),
            db_inserter=self.inserters.get_chunks_database_inserter()
        )

    def get_every_hit_manager(self,
                              client_session: ClientSession,
                              spotify_session: SpotifySession) -> EveryHitChartsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        tracks_collector = self.collectors.charts.get_tracks_collector(spotify_client)
        every_hit_charts_collector = self.collectors.charts.get_every_hit_collector(client_session)

        return EveryHitChartsManager(
            db_engine=get_database_engine(),
            charts_data_collector=every_hit_charts_collector,
            charts_tracks_collector=tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            chart_entries_inserter=self.inserters.get_chart_entries_inserter()
        )

    def get_eurovision_missing_tracks_manager(self, spotify_session: SpotifySession) -> EurovisionMissingTracksManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        missing_tracks_collector = self.collectors.charts.get_eurovision_missing_tracks_collector(spotify_client)

        return EurovisionMissingTracksManager(
            db_engine=get_database_engine(),
            missing_tracks_collector=missing_tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            db_updater=self.updaters.get_values_updater()
        )

    def _get_playlists_chart_manager(self,
                                     spotify_session: SpotifySession,
                                     playlist_id_to_chart_mapping: Dict[str, Chart]) -> PlaylistsChartsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        tracks_collector = self.collectors.charts.get_tracks_collector(spotify_client)
        data_collector = self.collectors.charts.get_playlists_charts_collector(
            spotify_client=spotify_client,
            playlist_id_to_chart_mapping=playlist_id_to_chart_mapping
        )

        return PlaylistsChartsManager(
            chart_entries_inserter=self.inserters.get_chart_entries_inserter(),
            charts_data_collector=data_collector,
            charts_tracks_collector=tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client)
        )

    def _get_radio_chart_manager(self,
                                 spotify_client: SpotifyClient,
                                 tracks_collector: ChartsTracksCollector) -> RadioChartsManager:
        drive_client = self.tools.get_google_drive_client()

        return RadioChartsManager(
            db_engine=get_database_engine(),
            drive_client=drive_client,
            charts_data_collector=self.collectors.charts.get_radio_charts_collector(drive_client),
            charts_tracks_collector=tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            chart_entries_inserter=self.inserters.get_chart_entries_inserter()
        )
