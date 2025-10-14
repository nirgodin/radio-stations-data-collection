from aiohttp import ClientSession

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import JosieCurationsManager


class CurationsManagerFactory(BaseManagerFactory):
    def get_josie_curations_manager(self, session: ClientSession) -> JosieCurationsManager:
        return JosieCurationsManager(
            josie_client=self.tools.get_josie_client(session),
            db_engine=self.tools.get_database_engine(),
        )
