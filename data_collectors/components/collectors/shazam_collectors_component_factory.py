from shazamio import Shazam

from data_collectors.logic.collectors.shazam import *
from data_collectors.tools import AioPoolExecutor


class ShazamCollectorsComponentFactory:
    @staticmethod
    def get_search_collector(shazam: Shazam, pool_executor: AioPoolExecutor) -> ShazamSearchCollector:
        return ShazamSearchCollector(shazam, pool_executor)

    @staticmethod
    def get_top_tracks_collector(shazam: Shazam, pool_executor: AioPoolExecutor) -> ShazamTopTracksCollector:
        return ShazamTopTracksCollector(shazam, pool_executor)

    @staticmethod
    def get_artists_collector(shazam: Shazam, pool_executor: AioPoolExecutor) -> ShazamArtistsCollector:
        return ShazamArtistsCollector(shazam, pool_executor)

    @staticmethod
    def get_tracks_collector(shazam: Shazam, pool_executor: AioPoolExecutor) -> ShazamTracksCollector:
        return ShazamTracksCollector(shazam, pool_executor)
