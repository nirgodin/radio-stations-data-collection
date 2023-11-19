from data_collectors import ShazamTopTracksCollector
from data_collectors.logic.collectors.shazam.shazam_artists_collector import ShazamArtistsCollector


class ShazamCollectorsComponentFactory:
    @staticmethod
    def get_top_tracks_collector() -> ShazamTopTracksCollector:
        return ShazamTopTracksCollector()

    @staticmethod
    def get_artists_collector() -> ShazamArtistsCollector:
        return ShazamArtistsCollector()
