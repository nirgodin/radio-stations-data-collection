from genie_datastores.postgres.operations import get_database_engine

from data_collectors.logic.inserters.postgres import BillboardChartsDatabaseInserter, BillboardTracksDatabaseInserter, \
    ChunksDatabaseInserter


class BillboardInsertersComponentFactory:
    @staticmethod
    def get_charts_inserter(chunks_inserter: ChunksDatabaseInserter) -> BillboardChartsDatabaseInserter:
        return BillboardChartsDatabaseInserter(chunks_inserter)

    @staticmethod
    def get_tracks_inserter() -> BillboardTracksDatabaseInserter:
        return BillboardTracksDatabaseInserter(get_database_engine())
