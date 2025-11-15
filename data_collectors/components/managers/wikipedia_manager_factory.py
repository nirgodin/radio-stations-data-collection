from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import *


class WikipediaManagerFactory(BaseManagerFactory):
    async def get_artists_about_manager(self) -> WikipediaArtistsAboutManager:
        return WikipediaArtistsAboutManager(
            db_engine=self.tools.get_database_engine(),
            pool_executor=self.tools.get_pool_executor(),
        )
