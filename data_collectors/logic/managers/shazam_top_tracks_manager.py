from itertools import chain

from data_collectors.contract import IManager
from data_collectors.consts.shazam_consts import KEY
from data_collectors.logic.collectors import ShazamTopTracksCollector
from data_collectors.logic.inserters import ShazamInsertionsManager
from data_collectors.logic.inserters import ShazamTopTracksDatabaseInserter
from data_collectors.logs import logger


class ShazamTopTracksManager(IManager):
    def __init__(self,
                 top_tracks_collector: ShazamTopTracksCollector,
                 insertions_manager: ShazamInsertionsManager,
                 top_tracks_inserter: ShazamTopTracksDatabaseInserter):
        self._top_tracks_collector = top_tracks_collector
        self._insertions_manager = insertions_manager
        self._top_tracks_inserter = top_tracks_inserter

    async def run(self):
        logger.info("Starting to run shazam top tracks manager")
        top_tracks = await self._top_tracks_collector.collect()
        flattened_tracks = list(chain.from_iterable(top_tracks.values()))
        tracks_ids = {track[KEY] for track in flattened_tracks}
        await self._insertions_manager.insert(list(tracks_ids))
        await self._top_tracks_inserter.insert(top_tracks)
