from postgres_client import get_database_engine

from data_collectors.logic.updaters import BillboardTracksDatabaseUpdater


class UpdatersComponentFactory:
    @staticmethod
    def get_tracks_updater() -> BillboardTracksDatabaseUpdater:
        return BillboardTracksDatabaseUpdater(get_database_engine())

    @staticmethod
    def get_shazam_ids_updater() -> ShazamIDsUpdater:
        return ShazamIDsUpdater(get_database_engine())
