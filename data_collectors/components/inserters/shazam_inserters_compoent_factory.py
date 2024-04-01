from genie_datastores.postgres.operations import get_database_engine

from data_collectors.logic.inserters.postgres import ShazamTopTracksDatabaseInserter, ShazamTracksDatabaseInserter, \
    ShazamArtistsDatabaseInserter, ChunksDatabaseInserter


class ShazamInsertersComponentFactory:
    @staticmethod
    def get_top_tracks_inserter(chunks_inserter: ChunksDatabaseInserter) -> ShazamTopTracksDatabaseInserter:
        return ShazamTopTracksDatabaseInserter(chunks_inserter)

    @staticmethod
    def get_tracks_inserter() -> ShazamTracksDatabaseInserter:
        return ShazamTracksDatabaseInserter(get_database_engine())

    @staticmethod
    def get_artists_inserter() -> ShazamArtistsDatabaseInserter:
        return ShazamArtistsDatabaseInserter(get_database_engine())
