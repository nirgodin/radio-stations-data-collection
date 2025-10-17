from typing import List

from genie_common.tools import logger
from spotipyio import SpotifyClient

from data_collectors.consts.spotify_consts import TRACK
from data_collectors.contract import IManager
from data_collectors.logic.inserters.postgres import CurationsInsertionManager, SpotifyInsertionsManager
from data_collectors.logic.models import Curation


class SpotifyUserPlaylistsCurationsManager(IManager):
    def __init__(
        self,
        spotify_client: SpotifyClient,
        spotify_insertions_manager: SpotifyInsertionsManager,
        curations_insertion_manager: CurationsInsertionManager,
    ):
        self._spotify_client = spotify_client
        self._spotify_insertions_manager = spotify_insertions_manager
        self._curations_insertion_manager = curations_insertion_manager

    async def run(self) -> None:
        logger.info("Collecting newly brewed Josie curations")
        print("b")
