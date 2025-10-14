from aiohttp import ClientSession

from data_collectors.components.managers.base_manager_factory import BaseManagerFactory
from data_collectors.logic.managers import JosieCurationsManager


class CurationsManagerFactory(BaseManagerFactory):
    def get_josie_curations_manager(self, session: ClientSession) -> JosieCurationsManager:
        return JosieCurationsManager(
            josie_curations_collector=self.collectors.curations.get_josie_collector(session),
            curations_insertion_manager=self.inserters.curations.get_curations_insertion_manager()
        )
