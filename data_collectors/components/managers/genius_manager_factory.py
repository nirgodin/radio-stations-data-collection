from aiohttp import ClientSession
from genie_datastores.postgres.operations import get_database_engine

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *


class GeniusManagerFactory(BaseManagerFactory):
    def get_missing_ids_manager(self, session: ClientSession) -> GeniusMissingIDsManager:
        pool_executor = self.tools.get_pool_executor()
        return GeniusMissingIDsManager(
            db_engine=get_database_engine(),
            search_collector=self.collectors.genius.get_search_collector(session, pool_executor),
            track_ids_updater=self.updaters.get_track_ids_updater()
        )
