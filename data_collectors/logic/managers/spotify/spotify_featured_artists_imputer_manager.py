from datetime import datetime
from typing import Optional, List, Dict

from genie_common.tools import logger
from genie_datastores.postgres.models import SpotifyTrack, SpotifyFeaturedArtist
from genie_datastores.postgres.operations import execute_query
from spotipyio import SpotifyClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.spotify_consts import TRACK
from data_collectors.contract import IManager
from data_collectors.logic.inserters.postgres import SpotifyInsertionsManager
from data_collectors.logic.models import DBUpdateRequest
from data_collectors.logic.updaters import ValuesDatabaseUpdater


class SpotifyFeaturedArtistImputerManager(IManager):
    def __init__(
        self,
        spotify_client: SpotifyClient,
        db_engine: AsyncEngine,
        spotify_insertions_manager: SpotifyInsertionsManager,
        db_updater: ValuesDatabaseUpdater,
    ):
        self._spotify_client = spotify_client
        self._db_engine = db_engine
        self._spotify_insertions_manager = spotify_insertions_manager
        self._db_updater = db_updater

    async def run(self, limit: Optional[int]) -> None:
        tracks_ids = await self._query_missing_tracks_ids(limit)
        tracks = await self._fetch_spotify_tracks_data(tracks_ids)
        await self._spotify_insertions_manager.insert(tracks)
        await self._update_spotify_tracks_update_date(tracks_ids)

    async def _query_missing_tracks_ids(self, limit: Optional[int]) -> List[str]:
        logger.info("Querying DB for Spotify tracks ids without featured artists")
        query = (
            select(SpotifyTrack.id)
            .where(SpotifyTrack.id.notin_(select(SpotifyFeaturedArtist.track_id)))
            .order_by(SpotifyTrack.update_date.asc())
            .limit(limit)
        )
        query_result = await execute_query(self._db_engine, query)

        return query_result.scalars().all()

    async def _fetch_spotify_tracks_data(self, ids: List[str]) -> List[Dict[str, dict]]:
        logger.info("Fetching Spotify for tracks data")
        tracks = await self._spotify_client.tracks.info.run(ids)

        return [{TRACK: track} for track in tracks]

    async def _insert_spotify_records(self, tracks: List[Dict[str, dict]]) -> None:
        await self._
        await self._featured_artists_inserter.insert(tracks)

    async def _update_spotify_tracks_update_date(self, tracks_ids: List[str]) -> None:
        logger.info("Updating Spotify tracks update date")
        now = datetime.utcnow()
        update_requests = [DBUpdateRequest(id=id_, values={SpotifyTrack.update_date: now}) for id_ in tracks_ids]

        await self._db_updater.update(update_requests)
