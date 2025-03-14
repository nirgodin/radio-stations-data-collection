from typing import Dict, Optional

from aiohttp import ClientSession
from genie_datastores.google.drive import GoogleDriveClient
from genie_datastores.postgres.models import Chart
from spotipyio import SpotifyClient

from data_collectors.components.tools_component_factory import ToolsComponentFactory
from data_collectors.logic.collectors import (
    BaseChartKeySearcher,
    ChartsTaggedMistakesCollector,
    ChartsTaggedMistakesTracksCollector,
    ChartsTracksCollector,
    EurovisionChartsDataCollector,
    EveryHitChartsDataCollector,
    GlglzChartsDataCollector,
    IsraeliChartKeySearcher,
    PlaylistsChartsDataCollector,
    RadioChartsDataCollector,
    EurovisionMissingTracksCollector,
)


class ChartsCollectorsComponentFactory:
    def __init__(self, tools: ToolsComponentFactory):
        self._tools = tools

    @staticmethod
    def get_radio_charts_collector(
        google_drive_client: GoogleDriveClient,
    ) -> RadioChartsDataCollector:
        return RadioChartsDataCollector(google_drive_client)

    def get_eurovision_charts_collector(
        self, session: ClientSession
    ) -> EurovisionChartsDataCollector:
        return EurovisionChartsDataCollector(
            pool_executor=self._tools.get_pool_executor(),
            wikipedia_text_collector=self._tools.get_wikipedia_text_collector(session),
        )

    def get_glglz_charts_collector(self) -> GlglzChartsDataCollector:
        return GlglzChartsDataCollector(self._tools.get_pool_executor())

    @staticmethod
    def get_playlists_charts_collector(
        spotify_client: SpotifyClient, playlist_id_to_chart_mapping: Dict[str, Chart]
    ) -> PlaylistsChartsDataCollector:
        return PlaylistsChartsDataCollector(
            spotify_client=spotify_client,
            playlist_id_to_chart_mapping=playlist_id_to_chart_mapping,
        )

    def get_tracks_collector(
        self,
        spotify_client: SpotifyClient,
        key_searcher: Optional[BaseChartKeySearcher] = None,
    ) -> ChartsTracksCollector:
        chart_key_searcher = key_searcher or IsraeliChartKeySearcher(
            spotify_client=spotify_client,
            entity_matcher=self._tools.get_multi_entity_matcher(),
        )
        return ChartsTracksCollector(
            pool_executor=self._tools.get_pool_executor(),
            spotify_client=spotify_client,
            db_engine=self._tools.get_database_engine(),
            key_searcher=chart_key_searcher,
        )

    def get_charts_tagged_mistakes_collector(self) -> ChartsTaggedMistakesCollector:
        return ChartsTaggedMistakesCollector(
            pool_executor=self._tools.get_pool_executor(),
            db_engine=self._tools.get_database_engine(),
        )

    def get_tagged_mistakes_tracks_collector(
        self,
        spotify_client: SpotifyClient,
    ) -> ChartsTaggedMistakesTracksCollector:
        return ChartsTaggedMistakesTracksCollector(
            db_engine=self._tools.get_database_engine(), spotify_client=spotify_client
        )

    def get_every_hit_collector(
        self, session: ClientSession
    ) -> EveryHitChartsDataCollector:
        return EveryHitChartsDataCollector(
            session=session, pool_executor=self._tools.get_pool_executor()
        )

    @staticmethod
    def get_eurovision_missing_tracks_collector(
        spotify_client: SpotifyClient,
    ) -> EurovisionMissingTracksCollector:
        return EurovisionMissingTracksCollector(spotify_client)
