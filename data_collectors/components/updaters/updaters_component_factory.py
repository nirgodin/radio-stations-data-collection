from genie_common.tools import AioPoolExecutor
from genie_datastores.postgres.operations import get_database_engine

from data_collectors.logic.updaters import TrackIDsDatabaseUpdater
from data_collectors.logic.updaters import BillboardTracksDatabaseUpdater
from data_collectors.logic.updaters.spotify_artists_database_updater import SpotifyArtistsDatabaseUpdater


class UpdatersComponentFactory:
    @staticmethod
    def get_billboard_tracks_updater() -> BillboardTracksDatabaseUpdater:
        return BillboardTracksDatabaseUpdater(get_database_engine())

    @staticmethod
    def get_track_ids_updater() -> TrackIDsDatabaseUpdater:
        return TrackIDsDatabaseUpdater(get_database_engine())

    @staticmethod
    def get_spotify_artists_updater(pool_executor: AioPoolExecutor) -> SpotifyArtistsDatabaseUpdater:
        return SpotifyArtistsDatabaseUpdater(db_engine=get_database_engine(), pool_executor=pool_executor)
