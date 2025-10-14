from genie_common.tools import logger

from data_collectors.contract import IManager
from data_collectors.logic.collectors import JosieCurationsCollector
from data_collectors.logic.inserters.postgres import CurationsInsertionManager


class JosieCurationsManager(IManager):
    def __init__(
        self, josie_curations_collector: JosieCurationsCollector, curations_insertion_manager: CurationsInsertionManager
    ):
        self._josie_curations_collector = josie_curations_collector
        self._curations_insertion_manager = curations_insertion_manager

    async def run(self) -> None:
        logger.info("Collecting newly brewed Josie curations")
        curations = await self._josie_curations_collector.collect()

        if curations:
            logger.info(f"Found {len(curations)} new Josie curations. Inserting")
            await self._curations_insertion_manager.insert(curations)

        else:
            logger.info("Did not find any new Josie curation. Aborting")
