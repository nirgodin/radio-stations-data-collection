from genie_common.tools import logger

from data_collectors.contract import IManager
from data_collectors.logic.collectors import ShazamTopTracksCollector
from data_collectors.logic.inserters import ShazamInsertionsManager
from data_collectors.logic.inserters.postgres import ShazamTopTracksDatabaseInserter


class ShazamTopTracksManager(IManager):
    def __init__(
        self,
        top_tracks_collector: ShazamTopTracksCollector,
        insertions_manager: ShazamInsertionsManager,
        top_tracks_inserter: ShazamTopTracksDatabaseInserter,
    ):
        self._top_tracks_collector = top_tracks_collector
        self._insertions_manager = insertions_manager
        self._top_tracks_inserter = top_tracks_inserter

    async def run(self):
        logger.info("Starting to run shazam top tracks manager")
        location_top_tracks_ids_map = await self._top_tracks_collector.collect()
        await self._top_tracks_inserter.insert(location_top_tracks_ids_map)
