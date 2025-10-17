from typing import List, Tuple, Any, Dict

from genie_common.tools import logger, AioPoolExecutor
from genie_datastores.postgres.models import CuratorCollection
from genie_datastores.postgres.operations import execute_query
from numpy.lib.function_base import iterable
from spotipyio import SpotifyClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.collectors import SpotifyPlaylistsCurationsCollector
from data_collectors.consts.spotify_consts import TRACK, ID, SNAPSHOT_ID
from data_collectors.contract import IManager
from data_collectors.logic.inserters.postgres import CurationsInsertionManager, SpotifyInsertionsManager
from data_collectors.logic.models import Curation

SPOTIFY_CURATORS_USER_IDS = ["guyhajaja"]


class SpotifyUserPlaylistsCurationsManager(IManager):
    def __init__(
        self,
        spotify_client: SpotifyClient,
        db_engine: AsyncEngine,
        spotify_playlists_curations_collector: SpotifyPlaylistsCurationsCollector,
        spotify_insertions_manager: SpotifyInsertionsManager,
        curations_insertion_manager: CurationsInsertionManager,
        pool_executor: AioPoolExecutor = AioPoolExecutor(),
    ):
        self._spotify_client = spotify_client
        self._db_engine = db_engine
        self._spotify_playlists_curations_collector = spotify_playlists_curations_collector
        self._spotify_insertions_manager = spotify_insertions_manager
        self._curations_insertion_manager = curations_insertion_manager
        self._pool_executor = pool_executor

    async def run(self) -> None:
        playlists_ids = await self._fetch_relevant_playlists_ids()

        if not playlists_ids:
            logger.info(f"Did not find any relevant playlist. Aborting")
            return

        logger.info(f"Found {len(playlists_ids)} relevant playlists. Starting to fetching curated tracks")
        curations = await self._spotify_playlists_curations_collector.collect(playlists_ids)
        print("b")

    async def _fetch_relevant_playlists_ids(self) -> List[str]:
        logger.info("Querying curators playlists")
        playlists = await self._spotify_client.users.playlists.run(ids=SPOTIFY_CURATORS_USER_IDS, max_pages=2)
        logger.info(f"Found {len(playlists)} users playlists. Filtering playlists with new tracks")
        playlists_relevance: List[Tuple[str, bool]] = self._pool_executor.run(
            iterable=playlists,
            func=self._is_relevant_playlist,
            expected_type=tuple,
        )

        return [playlist_id for playlist_id, is_relevant in playlists_relevance if is_relevant]

    async def _is_relevant_playlist(self, playlist: Dict[str, Any]) -> Tuple[str, bool]:
        playlist_id = playlist[ID]
        has_new_items = await self._has_new_items(playlist_id=playlist_id, current_snapshot_id=playlist[SNAPSHOT_ID])

        if has_new_items:
            logger.info(f"Playlist `{playlist_id}` has new tracks! Going to fetch new curated tracks")
        else:
            logger.info(f"Playlist `{playlist_id}` has no new tracks. Skipping")

        return playlist_id, has_new_items

    async def _has_new_items(self, playlist_id: str, current_snapshot_id: str) -> bool:
        query = select(CuratorCollection.comment).where(CuratorCollection.id == playlist_id).limit(1)
        cursor = await execute_query(engine=self._db_engine, query=query)
        stored_snapshot_id = cursor.scalars().first()

        return stored_snapshot_id != current_snapshot_id
