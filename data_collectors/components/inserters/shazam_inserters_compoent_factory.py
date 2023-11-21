from postgres_client import get_database_engine

from data_collectors import ShazamTopTracksDatabaseInserter


class ShazamInsertersComponentFactory:
    @staticmethod
    def get_top_tracks_inserter() -> ShazamTopTracksDatabaseInserter:
        return ShazamTopTracksDatabaseInserter(get_database_engine())
