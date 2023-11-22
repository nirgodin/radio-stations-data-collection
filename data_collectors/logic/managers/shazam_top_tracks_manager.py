from itertools import chain

from data_collectors import ShazamTopTracksCollector, ShazamTopTracksDatabaseInserter
from data_collectors.consts.spotify_consts import ID
from data_collectors.logic.inserters import ShazamInsertionsManager
from data_collectors.logs import logger


class ShazamTopTracksManager:
    def __init__(self,
                 shazam_top_tracks_collector: ShazamTopTracksCollector,
                 shazam_insertions_manager: ShazamInsertionsManager,
                 shazam_top_tracks_inserter: ShazamTopTracksDatabaseInserter):
        self._top_tracks_collector = shazam_top_tracks_collector
        self._insertions_manager = shazam_insertions_manager
        self._top_tracks_inserter = shazam_top_tracks_inserter

    async def run(self):
        logger.info("Starting to run shazam top tracks manager")
        top_tracks = await self._top_tracks_collector.collect()
        flattened_tracks = list(chain.from_iterable(top_tracks.values()))
        tracks_ids = [track[ID] for track in flattened_tracks]
        await self._insertions_manager.insert(tracks_ids)
        await self._top_tracks_inserter.insert(top_tracks)
