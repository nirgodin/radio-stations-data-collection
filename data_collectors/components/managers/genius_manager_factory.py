from aiohttp import ClientSession
from genie_datastores.postgres.operations import get_database_engine

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *


class GeniusManagerFactory(BaseManagerFactory):
    def get_missing_ids_manager(self, session: ClientSession) -> GeniusMissingIDsManager:
        return GeniusMissingIDsManager(
            db_engine=get_database_engine(),
            search_collector=self.collectors.genius.get_search_collector(session),
            track_ids_updater=self.updaters.get_track_ids_updater()
        )

    def get_artists_ids_manager(self, session: ClientSession) -> GeniusArtistsIDsManager:
        return GeniusArtistsIDsManager(
            db_engine=get_database_engine(),
            tracks_collector=self.collectors.genius.get_tracks_collector(session),
            db_updater=self.updaters.get_values_updater()
        )
