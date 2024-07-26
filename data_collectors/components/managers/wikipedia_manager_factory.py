from genie_datastores.mongo.operations import initialize_mongo
from genie_datastores.postgres.operations import get_database_engine

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *


class WikipediaManagerFactory(BaseManagerFactory):
    def get_artists_age_name_manager(self) -> WikipediaArtistsAgeManager:
        return WikipediaArtistsAgeManager(
            age_collector=self.collectors.wikipedia.get_wikipedia_age_name_collector(),
            age_analyzer=self.analyzers.get_wikipedia_age_analyzer(),
            db_updater=self.updaters.get_values_updater()
        )

    def get_artists_age_link_manager(self) -> WikipediaArtistsAgeManager:
        return WikipediaArtistsAgeManager(
            age_collector=self.collectors.wikipedia.get_wikipedia_age_link_collector(),
            age_analyzer=self.analyzers.get_wikipedia_age_analyzer(),
            db_updater=self.updaters.get_values_updater()
        )

    async def get_artists_about_manager(self) -> WikipediaArtistsAboutManager:
        await initialize_mongo()
        return WikipediaArtistsAboutManager(
            db_engine=get_database_engine(),
            pool_executor=self.tools.get_pool_executor()
        )
