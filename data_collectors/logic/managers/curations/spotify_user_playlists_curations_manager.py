from typing import List, Tuple, Any, Dict

from genie_common.tools import logger, AioPoolExecutor
from genie_common.utils import chain_lists
from genie_datastores.postgres.models import CuratorCollection
from genie_datastores.postgres.operations import execute_query
from spotipyio import SpotifyClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.spotify_consts import ID, SNAPSHOT_ID, ITEMS
from data_collectors.contract import IManager
from data_collectors.logic.collectors import SpotifyPlaylistsCurationsCollector
from data_collectors.logic.inserters.postgres import CurationsInsertionManager, SpotifyInsertionsManager
from data_collectors.logic.models import PlaylistCurations

SPOTIFY_CURATORS_NAMES_TO_USER_IDS = {
    "Nir Godin": "v4saf4cq6t00r5t5z0hupb7hu",
    "Guy Hajaj": "guyhajaja",
    "Kan 88": "r13s18fnwtz99s92ml28l40do",
    "Quami": "31a7qtpirrt4vpq55limgrfocfge",
}


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
            logger.info("Did not find any relevant playlist. Aborting")
            return

        logger.info(f"Found {len(playlists_ids)} relevant playlists. Starting to fetching curated tracks")
        playlists_curations = await self._spotify_playlists_curations_collector.collect(playlists_ids)
        await self._insert_playlist_curations(playlists_curations)

    async def _fetch_relevant_playlists_ids(self) -> List[str]:
        logger.info("Querying curators playlists")
        relevant_playlists = []
        users_playlists = await self._spotify_client.users.playlists.run(
            ids=list(SPOTIFY_CURATORS_NAMES_TO_USER_IDS.values()), max_pages=2
        )

        for user_playlists in users_playlists:
            user_relevant_playlists = await self._extract_user_relevant_playlists(user_playlists)
            relevant_playlists.extend(user_relevant_playlists)

        return relevant_playlists

    async def _extract_user_relevant_playlists(self, user_playlists: Dict[str, Any]) -> List[str]:
        playlists = user_playlists[ITEMS]
        logger.info(f"Found {len(playlists)} users playlists. Filtering playlists with new tracks")
        playlists_relevance: List[Tuple[str, bool]] = await self._pool_executor.run(
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

    async def _insert_playlist_curations(self, playlists_curations: List[PlaylistCurations]) -> None:
        tracks = chain_lists([curations.tracks for curations in playlists_curations])
        if tracks:
            await self._spotify_insertions_manager.insert(tracks)

        curations = chain_lists([curations.curations for curations in playlists_curations])
        if curations:
            await self._curations_insertion_manager.insert(curations)
