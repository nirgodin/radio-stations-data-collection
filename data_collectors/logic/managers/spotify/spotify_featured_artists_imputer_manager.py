from typing import Optional, List

from genie_datastores.postgres.models import SpotifyTrack, SpotifyFeaturedArtist
from genie_datastores.postgres.operations import execute_query
from spotipyio import SpotifyClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IManager


class SpotifyFeaturedArtistImputerManager(IManager):
    def __init__(self, spotify_client: SpotifyClient, db_engine: AsyncEngine):
        self._spotify_client = spotify_client
        self._db_engine = db_engine

    async def run(self, limit: Optional[int]) -> None:
        tracks_ids = await self._query_missing_tracks_ids(limit)

    async def _query_missing_tracks_ids(self, limit: Optional[int]) -> List[str]:
        query = (
            select(SpotifyTrack.id)
            .where(SpotifyTrack.id.notin_(select(SpotifyFeaturedArtist.track_id)))
            .order_by(SpotifyTrack.update_date.asc())
            .limit(limit)
        )
        query_result = await execute_query(self._db_engine, query)

        return query_result.scalars().all()
