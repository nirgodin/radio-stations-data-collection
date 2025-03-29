from abc import ABC, abstractmethod
from typing import List, Dict, Any, Set, Generator

from genie_common.tools import logger
from genie_datastores.postgres.models import BaseORMModel
from spotipyio import SpotifyClient

from data_collectors.consts.spotify_consts import ID
from data_collectors.logic.models import DBUpdateRequest
from data_collectors.logic.updaters import ValuesDatabaseUpdater
from data_collectors.contract import IManager


class BaseSpotifyPlaylistsManager(IManager, ABC):
    def __init__(self, spotify_client: SpotifyClient, db_updater: ValuesDatabaseUpdater):
        self._spotify_client = spotify_client
        self._db_updater = db_updater

    async def run(self, playlists_ids: List[str], values: Dict[BaseORMModel, Any]) -> None:
        playlists = await self._spotify_client.playlists.info.run(playlists_ids)
        valid_playlists = self._filter_out_invalid_playlists(playlists)
        unique_ids = self._extract_playlists_ids(valid_playlists)
        update_requests = [DBUpdateRequest(id=id_, values=values) for id_ in unique_ids]

        await self._db_updater.update(update_requests)

    @staticmethod
    def _filter_out_invalid_playlists(playlists: List[dict]) -> List[dict]:
        valid_playlists = [playlist for playlist in playlists if isinstance(playlist, dict)]
        n_playlists = len(playlists)
        n_valid_playlists = len(valid_playlists)

        if n_valid_playlists < n_playlists:
            n_invalid_playlists = n_playlists - n_valid_playlists
            logger.warn(f"Found {n_invalid_playlists} invalid playlists. Filtering them out")

        return valid_playlists

    def _extract_playlists_ids(self, playlists: List[dict]) -> Set[str]:
        ids = set()

        for playlist in playlists:
            logger.info(f"Starting to extract playlist `{playlist[ID]}` ids")

            for id_ in self._extract_single_playlist_ids(playlist):
                ids.add(id_)

        return ids

    @abstractmethod
    def _extract_single_playlist_ids(self, playlist: dict) -> Generator[str, None, None]:
        raise NotImplementedError
