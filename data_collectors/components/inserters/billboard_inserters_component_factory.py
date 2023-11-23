from postgres_client import get_database_engine

from data_collectors import BillboardChartsDatabaseInserter, BillboardTracksDatabaseInserter


class BillboardInsertersComponentFactory:
    @staticmethod
    def get_charts_inserter() -> BillboardChartsDatabaseInserter:
        return BillboardChartsDatabaseInserter(get_database_engine())

    @staticmethod
    def get_tracks_inserter() -> BillboardTracksDatabaseInserter:
        return BillboardTracksDatabaseInserter(get_database_engine())
