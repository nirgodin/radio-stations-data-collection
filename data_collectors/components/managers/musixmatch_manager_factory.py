from aiohttp import ClientSession
from genie_datastores.postgres.operations import get_database_engine

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *


class MusixmatchManagerFactory(BaseManagerFactory):
    def get_missing_ids_manager(self, session: ClientSession) -> MusixmatchMissingIDsManager:
        pool_executor = self.tools.get_pool_executor()
        api_key = self.env.get_musixmatch_api_key()

        return MusixmatchMissingIDsManager(
            db_engine=get_database_engine(),
            search_collector=self.collectors.musixmatch.get_search_collector(session, pool_executor, api_key),
            track_ids_updater=self.updaters.get_track_ids_updater(),
        )
