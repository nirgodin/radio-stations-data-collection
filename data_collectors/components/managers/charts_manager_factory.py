from typing import Dict

from aiohttp import ClientSession
from genie_datastores.postgres.models import Chart
from genie_datastores.postgres.operations import get_database_engine
from playwright.async_api import Browser
from spotipyio import SpotifyClient
from spotipyio.tools.matching import EntityMatcher
from spotipyio.tools.extractors import (
    TrackEntityExtractor,
    PrimaryArtistEntityExtractor,
)
from spotipyio.auth import SpotifySession

from data_collectors.consts.charts_consts import (
    SPOTIFY_PLAYLIST_CHART_MAP,
    MAKO_PLAYLIST_CHART_MAP,
    BILLBOARD_PLAYLIST_CHART_MAP,
)
from data_collectors.logic.collectors import (
    ChartsTracksCollector,
    ArtistTranslatorChartKeySearcher,
)
from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *


class ChartsManagerFactory(BaseManagerFactory):
    def get_radio_charts_manager(self, spotify_session: SpotifySession) -> RadioChartsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        tracks_collector = self.collectors.charts.get_tracks_collector(spotify_client)

        return self._get_radio_chart_manager(spotify_client, tracks_collector)

    def get_translated_artist_radio_charts_manager(self, spotify_session: SpotifySession) -> RadioChartsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        extractors = {TrackEntityExtractor(): 0.5, PrimaryArtistEntityExtractor(): 0.5}
        entity_matcher = EntityMatcher(extractors=extractors, threshold=0.75)
        key_searcher = ArtistTranslatorChartKeySearcher(
            spotify_client=spotify_client,
            translation_adapter=self.tools.get_translation_adapter(),
            entity_matcher=self.tools.get_multi_entity_matcher(entity_matcher),
        )
        tracks_collector = self.collectors.charts.get_tracks_collector(
            spotify_client=spotify_client, key_searcher=key_searcher
        )

        return self._get_radio_chart_manager(spotify_client, tracks_collector)

    def get_eurovision_charts_manager(
        self, client_session: ClientSession, spotify_session: SpotifySession
    ) -> EurovisionChartsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        tracks_collector = self.collectors.charts.get_tracks_collector(spotify_client)
        eurovision_charts_collector = self.collectors.charts.get_eurovision_charts_collector(client_session)

        return EurovisionChartsManager(
            db_engine=self.tools.get_database_engine(),
            charts_data_collector=eurovision_charts_collector,
            charts_tracks_collector=tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            chart_entries_inserter=self.inserters.get_chart_entries_inserter(),
        )

    def get_glglz_charts_manager(self, spotify_session: SpotifySession, browser: Browser) -> GlglzChartsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        tracks_collector = self.collectors.charts.get_tracks_collector(spotify_client)

        return GlglzChartsManager(
            chart_entries_inserter=self.inserters.get_chart_entries_inserter(),
            charts_data_collector=self.collectors.charts.get_glglz_charts_collector(browser),
            charts_tracks_collector=tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            browser=browser,
            db_engine=self.tools.get_database_engine(),
            glz_base_url=self.env.get_glz_base_url(),
        )

    def get_spotify_charts_manager(self, spotify_session: SpotifySession) -> PlaylistsChartsManager:
        return self._get_playlists_chart_manager(
            spotify_session=spotify_session,
            playlist_id_to_chart_mapping=SPOTIFY_PLAYLIST_CHART_MAP,
        )

    def get_mako_hit_list_charts_manager(self, spotify_session: SpotifySession) -> PlaylistsChartsManager:
        return self._get_playlists_chart_manager(
            spotify_session=spotify_session,
            playlist_id_to_chart_mapping=MAKO_PLAYLIST_CHART_MAP,
        )

    def get_billboard_charts_manager(self, spotify_session: SpotifySession) -> PlaylistsChartsManager:
        return self._get_playlists_chart_manager(
            spotify_session=spotify_session,
            playlist_id_to_chart_mapping=BILLBOARD_PLAYLIST_CHART_MAP,
        )

    def get_tagged_mistakes_manager(self, spotify_session: SpotifySession) -> ChartsTaggedMistakesManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        tagged_mistakes_tracks_collector = self.collectors.charts.get_tagged_mistakes_tracks_collector(spotify_client)

        return ChartsTaggedMistakesManager(
            sheets_client=self.tools.get_google_sheets_client(),
            tagged_mistakes_collector=self.collectors.charts.get_charts_tagged_mistakes_collector(),
            tagged_mistakes_tracks_collector=tagged_mistakes_tracks_collector,
            db_updater=self.updaters.get_values_updater(),
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
        )

    def get_israeli_artists_manager(self) -> ChartsIsraeliArtistsManager:
        return ChartsIsraeliArtistsManager(
            db_engine=get_database_engine(),
            db_updater=self.updaters.get_values_updater(),
            db_inserter=self.inserters.get_chunks_database_inserter(),
        )

    def get_every_hit_manager(
        self, client_session: ClientSession, spotify_session: SpotifySession
    ) -> EveryHitChartsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        tracks_collector = self.collectors.charts.get_tracks_collector(spotify_client)
        every_hit_charts_collector = self.collectors.charts.get_every_hit_collector(client_session)

        return EveryHitChartsManager(
            db_engine=get_database_engine(),
            charts_data_collector=every_hit_charts_collector,
            charts_tracks_collector=tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            chart_entries_inserter=self.inserters.get_chart_entries_inserter(),
        )

    def get_eurovision_missing_tracks_manager(self, spotify_session: SpotifySession) -> EurovisionMissingTracksManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        missing_tracks_collector = self.collectors.charts.get_eurovision_missing_tracks_collector(spotify_client)

        return EurovisionMissingTracksManager(
            db_engine=get_database_engine(),
            missing_tracks_collector=missing_tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            db_updater=self.updaters.get_values_updater(),
        )

    def _get_playlists_chart_manager(
        self,
        spotify_session: SpotifySession,
        playlist_id_to_chart_mapping: Dict[str, Chart],
    ) -> PlaylistsChartsManager:
        spotify_client = self.tools.get_spotify_client(spotify_session)
        tracks_collector = self.collectors.charts.get_tracks_collector(spotify_client)
        data_collector = self.collectors.charts.get_playlists_charts_collector(
            spotify_client=spotify_client,
            playlist_id_to_chart_mapping=playlist_id_to_chart_mapping,
        )

        return PlaylistsChartsManager(
            chart_entries_inserter=self.inserters.get_chart_entries_inserter(),
            charts_data_collector=data_collector,
            charts_tracks_collector=tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
        )

    def _get_radio_chart_manager(
        self, spotify_client: SpotifyClient, tracks_collector: ChartsTracksCollector
    ) -> RadioChartsManager:
        drive_client = self.tools.get_google_drive_client()

        return RadioChartsManager(
            db_engine=get_database_engine(),
            drive_client=drive_client,
            charts_data_collector=self.collectors.charts.get_radio_charts_collector(drive_client),
            charts_tracks_collector=tracks_collector,
            spotify_insertions_manager=self.inserters.spotify.get_insertions_manager(spotify_client),
            chart_entries_inserter=self.inserters.get_chart_entries_inserter(),
        )
