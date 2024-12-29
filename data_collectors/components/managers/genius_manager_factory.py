from aiohttp import ClientSession
from genie_datastores.mongo.operations import initialize_mongo
from genie_datastores.postgres.operations import get_database_engine

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *
from data_collectors.logic.models import GeniusTextFormat


class GeniusManagerFactory(BaseManagerFactory):
    def get_missing_ids_manager(
        self, session: ClientSession
    ) -> GeniusMissingIDsManager:
        return GeniusMissingIDsManager(
            db_engine=get_database_engine(),
            search_collector=self.collectors.genius.get_search_collector(session),
            track_ids_updater=self.updaters.get_track_ids_updater(),
        )

    def get_artists_ids_manager(
        self, session: ClientSession
    ) -> GeniusArtistsIDsManager:
        return GeniusArtistsIDsManager(
            db_engine=get_database_engine(),
            tracks_collector=self.collectors.genius.get_tracks_collector(session),
            db_updater=self.updaters.get_values_updater(),
        )

    async def get_artists_manager(self, session: ClientSession) -> GeniusArtistsManager:
        await initialize_mongo()
        return GeniusArtistsManager(
            db_engine=get_database_engine(),
            artists_collector=self.collectors.genius.get_artists_collector(session),
            chunks_inserter=self.inserters.get_chunks_database_inserter(),
            text_format=GeniusTextFormat.PLAIN,
        )
