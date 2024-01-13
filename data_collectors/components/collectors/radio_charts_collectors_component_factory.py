from genie_common.tools import AioPoolExecutor
from genie_datastores.google_drive.google_drive_client import GoogleDriveClient
from genie_datastores.postgres.operations import get_database_engine
from spotipyio import SpotifyClient

from data_collectors.logic.collectors import (
    RadioChartsDataCollector,
    GlglzChartsDataCollector,
    RadioChartsTracksCollector
)


class RadioChartsCollectorsComponentFactory:
    @staticmethod
    def get_charts_collector(google_drive_client: GoogleDriveClient) -> RadioChartsDataCollector:
        return RadioChartsDataCollector(google_drive_client)

    @staticmethod
    def get_glglz_charts_collector() -> GlglzChartsDataCollector:
        return GlglzChartsDataCollector()

    @staticmethod
    def get_tracks_collector(pool_executor: AioPoolExecutor,
                             spotify_client: SpotifyClient) -> RadioChartsTracksCollector:
        return RadioChartsTracksCollector(
            pool_executor=pool_executor,
            spotify_client=spotify_client,
            db_engine=get_database_engine()
        )
