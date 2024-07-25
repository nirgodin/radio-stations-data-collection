from genie_common.tools import AioPoolExecutor
from genie_datastores.postgres.operations import get_database_engine
from shazamio import Shazam

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.inserters.postgres import ShazamInsertionsManager
from data_collectors.logic.managers import *


class ShazamManagerFactory(BaseManagerFactory):
    async def get_top_tracks_manager(self) -> ShazamTopTracksManager:
        shazam = self.tools.get_shazam()
        pool_executor = self.tools.get_pool_executor()
        chunks_inserter = self.inserters.get_chunks_database_inserter()
        insertions_manager = await self.get_insertions_manager(shazam, pool_executor)

        return ShazamTopTracksManager(
            top_tracks_collector=self.collectors.shazam.get_top_tracks_collector(shazam, pool_executor),
            insertions_manager=insertions_manager,
            top_tracks_inserter=self.inserters.shazam.get_top_tracks_inserter(chunks_inserter)
        )

    async def get_missing_ids_manager(self) -> ShazamMissingIDsManager:
        shazam = self.tools.get_shazam()
        pool_executor = self.tools.get_pool_executor()
        insertions_manager = await self.get_insertions_manager(shazam, pool_executor)

        return ShazamMissingIDsManager(
            db_engine=get_database_engine(),
            search_collector=self.collectors.shazam.get_search_collector(shazam, pool_executor),
            insertions_manager=insertions_manager,
            track_ids_updater=self.updaters.get_track_ids_updater(),
        )

    async def get_insertions_manager(self, shazam: Shazam, pool_executor: AioPoolExecutor) -> ShazamInsertionsManager:
        artists_inserter = await self.inserters.shazam.get_artists_inserter()
        return ShazamInsertionsManager(
            artists_collector=self.collectors.shazam.get_artists_collector(shazam, pool_executor),
            tracks_collector=self.collectors.shazam.get_tracks_collector(shazam, pool_executor),
            artists_inserter=artists_inserter,
            tracks_inserter=self.inserters.shazam.get_tracks_inserter()
        )

    def get_birth_date_copy_manager(self) -> ShazamBirthDateCopyManager:
        return ShazamBirthDateCopyManager(
            db_engine=get_database_engine(),
            db_updater=self.updaters.get_values_updater()
        )

    def get_origin_copy_manager(self) -> ShazamOriginCopyManager:
        return ShazamOriginCopyManager(
            db_engine=get_database_engine(),
            db_updater=self.updaters.get_values_updater(),
            db_inserter=self.inserters.get_chunks_database_inserter()
        )
