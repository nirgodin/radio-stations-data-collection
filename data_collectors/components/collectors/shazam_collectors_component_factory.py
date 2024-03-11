from genie_common.tools import AioPoolExecutor
from shazamio import Shazam
from spotipyio import EntityMatcher

from data_collectors.logic.collectors.shazam import *
from data_collectors.tools import ShazamTrackEntityExtractor, ShazamArtistEntityExtractor, MultiEntityMatcher


class ShazamCollectorsComponentFactory:
    @staticmethod
    def get_search_collector(shazam: Shazam, pool_executor: AioPoolExecutor) -> ShazamSearchCollector:
        entity_matcher = EntityMatcher(
            {
                ShazamTrackEntityExtractor(): 0.7,
                ShazamArtistEntityExtractor(): 0.3,
            }
        )
        return ShazamSearchCollector(
            shazam=shazam,
            pool_executor=pool_executor,
            entity_matcher=MultiEntityMatcher(entity_matcher)
        )

    @staticmethod
    def get_top_tracks_collector(shazam: Shazam, pool_executor: AioPoolExecutor) -> ShazamTopTracksCollector:
        return ShazamTopTracksCollector(shazam, pool_executor)

    @staticmethod
    def get_artists_collector(shazam: Shazam, pool_executor: AioPoolExecutor) -> ShazamArtistsCollector:
        return ShazamArtistsCollector(shazam, pool_executor)

    @staticmethod
    def get_tracks_collector(shazam: Shazam, pool_executor: AioPoolExecutor) -> ShazamTracksCollector:
        return ShazamTracksCollector(shazam, pool_executor)

    @staticmethod
    def get_lyrics_collector(pool_executor: AioPoolExecutor) -> ShazamLyricsCollector:
        return ShazamLyricsCollector(pool_executor)
