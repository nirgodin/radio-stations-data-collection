from postgres_client import get_database_engine

from data_collectors import ShazamTracksDatabaseInserter


class ShazamInsertersComponentFactory:
    @staticmethod
    def get_tracks_inserter() -> ShazamTracksDatabaseInserter:
        return ShazamTracksDatabaseInserter(get_database_engine())
