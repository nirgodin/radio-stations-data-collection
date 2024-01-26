from typing import Dict

from genie_common.tools import AioPoolExecutor
from genie_datastores.google.drive import GoogleDriveClient
from genie_datastores.postgres.models import Chart
from genie_datastores.postgres.operations import get_database_engine
from spotipyio import SpotifyClient

from data_collectors.logic.collectors import (
    RadioChartsDataCollector,
    GlglzChartsDataCollector,
    RadioChartsTracksCollector,
    PlaylistsChartsDataCollector,
)


class RadioChartsCollectorsComponentFactory:
    @staticmethod
    def get_charts_collector(google_drive_client: GoogleDriveClient) -> RadioChartsDataCollector:
        return RadioChartsDataCollector(google_drive_client)

    @staticmethod
    def get_glglz_charts_collector() -> GlglzChartsDataCollector:
        return GlglzChartsDataCollector()

    @staticmethod
    def get_playlists_charts_collector(spotify_client: SpotifyClient,
                                       playlist_id_to_chart_mapping: Dict[str, Chart]) -> PlaylistsChartsDataCollector:
        return PlaylistsChartsDataCollector(
            spotify_client=spotify_client,
            playlist_id_to_chart_mapping=playlist_id_to_chart_mapping
        )

    @staticmethod
    def get_tracks_collector(pool_executor: AioPoolExecutor,
                             spotify_client: SpotifyClient) -> RadioChartsTracksCollector:
        return RadioChartsTracksCollector(
            pool_executor=pool_executor,
            spotify_client=spotify_client,
            db_engine=get_database_engine()
        )
