import math
from datetime import datetime, timedelta
from typing import List

from genie_common.tools import logger
from genie_common.utils import safe_nested_get
from genie_datastores.postgres.models import SpotifyTrack
from genie_datastores.postgres.operations import execute_query
from spotipyio import SpotifyClient
from spotipyio.logic.utils import to_uri
from spotipyio.models import EntityType as SpotifyEntityType
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.spotify_consts import TRACKS, ITEMS, TRACK, URI, SNAPSHOT_ID
from data_collectors.contract import IManager


class ReleaseRadarManager(IManager):
    def __init__(
        self,
        db_engine: AsyncEngine,
        spotify_client: SpotifyClient,
        playlist_id: str,
        release_range: timedelta = timedelta(weeks=1),
    ):
        self._db_engine = db_engine
        self._spotify_client = spotify_client
        self._playlist_id = playlist_id
        self._release_range = release_range

    async def run(self):
        logger.info("Starting to run ReleaseRadar manager")
        await self._remove_existing_playlist_items()
        track_ids = await self._query_newly_released_tracks()
        await self._add_new_playlist_items(track_ids)

    async def _remove_existing_playlist_items(self) -> None:
        logger.info("Fetching existing release radar playlist")
        playlist = await self._spotify_client.playlists.info.run_single(
            id_=self._playlist_id, max_pages=math.inf
        )
        existing_items = safe_nested_get(playlist, [TRACKS, ITEMS])
        existing_uris = [safe_nested_get(item, [TRACK, URI]) for item in existing_items]
        logger.info("Removing all existing items from playlist")

        await self._spotify_client.playlists.remove_items.run(
            playlist_id=self._playlist_id,
            uris=existing_uris,
            snapshot_id=playlist[SNAPSHOT_ID],
        )

    async def _add_new_playlist_items(self, track_ids: List[str]) -> None:
        if not track_ids:
            logger.info("Did not find any new track. Aborting playlist update")
            return

        uris = [to_uri(track_id, SpotifyEntityType.TRACK) for track_id in track_ids]
        await self._spotify_client.playlists.add_items.run(
            playlist_id=self._playlist_id, uris=uris, position=0
        )

    async def _query_newly_released_tracks(self) -> List[str]:
        logger.info("Querying newly released tracks from database")
        week_ago = datetime.now() - self._release_range
        query = (
            select(SpotifyTrack.id)
            .distinct(SpotifyTrack.id)
            .where(SpotifyTrack.release_date >= week_ago)
        )
        cursor = await execute_query(query=query, engine=self._db_engine)
        tracks_ids = cursor.scalars().all()
        logger.info(f"Found {len(tracks_ids)} newly released tracks in database")

        return tracks_ids
