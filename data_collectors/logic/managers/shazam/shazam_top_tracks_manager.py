from itertools import chain

from genie_common.tools import logger

from data_collectors.consts.spotify_consts import ID
from data_collectors.contract import IManager
from data_collectors.logic.collectors import ShazamTopTracksCollector
from data_collectors.logic.inserters.postgres import ShazamTopTracksDatabaseInserter
from data_collectors.logic.inserters import ShazamInsertionsManager


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
        tracks_ids = {track[ID] for track in flattened_tracks}
        await self._insertions_manager.insert(list(tracks_ids))
        await self._top_tracks_inserter.insert(top_tracks)
