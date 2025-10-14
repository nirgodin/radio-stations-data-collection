from typing import List

from genie_common.tools import logger
from spotipyio import SpotifyClient

from data_collectors.consts.spotify_consts import TRACK
from data_collectors.contract import IManager
from data_collectors.logic.collectors import JosieCurationsCollector
from data_collectors.logic.inserters.postgres import CurationsInsertionManager, SpotifyInsertionsManager
from data_collectors.logic.models import Curation


class JosieCurationsManager(IManager):
    def __init__(
        self,
        josie_curations_collector: JosieCurationsCollector,
        spotify_client: SpotifyClient,
        spotify_insertions_manager: SpotifyInsertionsManager,
        curations_insertion_manager: CurationsInsertionManager,
    ):
        self._josie_curations_collector = josie_curations_collector
        self._spotify_client = spotify_client
        self._spotify_insertions_manager = spotify_insertions_manager
        self._curations_insertion_manager = curations_insertion_manager

    async def run(self) -> None:
        logger.info("Collecting newly brewed Josie curations")
        curations = await self._josie_curations_collector.collect()

        if curations:
            logger.info(f"Found {len(curations)} new Josie curations. Fetching relevant data and inserting")
            await self._insert_spotify_tracks(curations)
            await self._curations_insertion_manager.insert(curations)

        else:
            logger.info("Did not find any new Josie curation. Aborting")

    async def _insert_spotify_tracks(self, curations: List[Curation]) -> None:
        logger.info("Fetching spotify tracks to insert missing tracks before inserting curations")
        tracks_ids = [curation.track_id for curation in curations]
        tracks_info = await self._spotify_client.tracks.info.run(tracks_ids)
        tracks = [{TRACK: track} for track in tracks_info]

        await self._spotify_insertions_manager.insert(tracks)
