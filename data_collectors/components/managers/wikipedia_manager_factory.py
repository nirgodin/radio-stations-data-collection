from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *


class WikipediaManagerFactory(BaseManagerFactory):
    def get_artists_age_name_manager(self) -> WikipediaArtistsAgeManager:
        pool_executor = self.tools.get_pool_executor()
        age_collector = self.collectors.wikipedia.get_wikipedia_age_name_collector(pool_executor)
        artists_updater = self.updaters.get_artists_updater(pool_executor)

        return WikipediaArtistsAgeManager(
            age_collector=age_collector,
            artists_updater=artists_updater
        )

    def get_artists_age_link_manager(self) -> WikipediaArtistsAgeManager:
        pool_executor = self.tools.get_pool_executor()
        age_collector = self.collectors.wikipedia.get_wikipedia_age_link_collector(pool_executor)
        artists_updater = self.updaters.get_artists_updater(pool_executor)

        return WikipediaArtistsAgeManager(
            age_collector=age_collector,
            artists_updater=artists_updater
        )
