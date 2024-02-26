from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *


class WikipediaManagerFactory(BaseManagerFactory):
    def get_artists_age_name_manager(self) -> WikipediaArtistsAgeManager:
        pool_executor = self.tools.get_pool_executor()
        age_collector = self.collectors.wikipedia.get_wikipedia_age_name_collector(pool_executor)

        return WikipediaArtistsAgeManager(
            age_collector=age_collector,
            age_analyzer=self.analyzers.get_wikipedia_age_analyzer(),
            db_updater=self.updaters.get_values_updater()
        )

    def get_artists_age_link_manager(self) -> WikipediaArtistsAgeManager:
        pool_executor = self.tools.get_pool_executor()
        age_collector = self.collectors.wikipedia.get_wikipedia_age_link_collector(pool_executor)

        return WikipediaArtistsAgeManager(
            age_collector=age_collector,
            age_analyzer=self.analyzers.get_wikipedia_age_analyzer(),
            db_updater=self.updaters.get_values_updater()
        )
