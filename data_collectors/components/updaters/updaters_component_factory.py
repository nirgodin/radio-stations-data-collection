from postgres_client import get_database_engine

from data_collectors.logic.updaters import TrackIDsDatabaseUpdater
from data_collectors.logic.updaters import BillboardTracksDatabaseUpdater


class UpdatersComponentFactory:
    @staticmethod
    def get_billboard_tracks_updater() -> BillboardTracksDatabaseUpdater:
        return BillboardTracksDatabaseUpdater(get_database_engine())

    @staticmethod
    def get_track_ids_updater() -> TrackIDsDatabaseUpdater:
        return TrackIDsDatabaseUpdater(get_database_engine())
