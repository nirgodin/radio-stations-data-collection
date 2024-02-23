from typing import Dict

from aiohttp import ClientSession
from genie_common.tools import AioPoolExecutor
from genie_datastores.google.drive import GoogleDriveClient
from genie_datastores.postgres.models import Chart
from genie_datastores.postgres.operations import get_database_engine
from spotipyio import SpotifyClient

from data_collectors.logic.collectors import (
    ChartsTaggedMistakesCollector,
    ChartsTaggedMistakesTracksCollector,
    ChartsTracksCollector,
    EurovisionChartsDataCollector,
    EveryHitChartsDataCollector,
    GlglzChartsDataCollector,
    PlaylistsChartsDataCollector,
    RadioChartsDataCollector,
)


class ChartsCollectorsComponentFactory:
    @staticmethod
    def get_radio_charts_collector(google_drive_client: GoogleDriveClient) -> RadioChartsDataCollector:
        return RadioChartsDataCollector(google_drive_client)

    @staticmethod
    def get_eurovision_charts_collector(session: ClientSession,
                                        pool_executor: AioPoolExecutor) -> EurovisionChartsDataCollector:
        return EurovisionChartsDataCollector(
            session=session,
            pool_executor=pool_executor
        )

    @staticmethod
    def get_glglz_charts_collector(pool_executor: AioPoolExecutor) -> GlglzChartsDataCollector:
        return GlglzChartsDataCollector(pool_executor)

    @staticmethod
    def get_playlists_charts_collector(spotify_client: SpotifyClient,
                                       playlist_id_to_chart_mapping: Dict[str, Chart]) -> PlaylistsChartsDataCollector:
        return PlaylistsChartsDataCollector(
            spotify_client=spotify_client,
            playlist_id_to_chart_mapping=playlist_id_to_chart_mapping
        )

    @staticmethod
    def get_tracks_collector(pool_executor: AioPoolExecutor,
                             spotify_client: SpotifyClient) -> ChartsTracksCollector:
        return ChartsTracksCollector(
            pool_executor=pool_executor,
            spotify_client=spotify_client,
            db_engine=get_database_engine()
        )

    @staticmethod
    def get_charts_tagged_mistakes_collector(pool_executor: AioPoolExecutor) -> ChartsTaggedMistakesCollector:
        return ChartsTaggedMistakesCollector(
            pool_executor=pool_executor,
            db_engine=get_database_engine()
        )

    @staticmethod
    def get_tagged_mistakes_tracks_collector(spotify_client: SpotifyClient) -> ChartsTaggedMistakesTracksCollector:
        return ChartsTaggedMistakesTracksCollector(
            db_engine=get_database_engine(),
            spotify_client=spotify_client
        )

    @staticmethod
    def get_every_hit_collector(session: ClientSession, pool_executor: AioPoolExecutor) -> EveryHitChartsDataCollector:
        return EveryHitChartsDataCollector(
            session=session,
            pool_executor=pool_executor
        )
