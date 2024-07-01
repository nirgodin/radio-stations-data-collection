from typing import Optional, Dict

from genie_datastores.postgres.models import TrackIDMapping, SpotifyArtist, SpotifyTrack
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.collectors import GeniusTracksCollector
from data_collectors.contract import IManager
from data_collectors.logic.models import GeniusTextFormat


class GeniusArtistsIDsManager(IManager):
    def __init__(self, db_engine: AsyncEngine, tracks_collector: GeniusTracksCollector):
        self._db_engine = db_engine
        self._tracks_collector = tracks_collector

    async def run(self, limit: Optional[int]) -> None:
        genius_id_artist_id_mapping = await self._query_tracks_ids(limit)
        track_ids = list(genius_id_artist_id_mapping.keys())
        tracks = await self._tracks_collector.collect(track_ids, GeniusTextFormat.PLAIN)
        print('b')

    async def _query_tracks_ids(self, limit: Optional[int]) -> Dict[str, str]:
        query = (
            select(TrackIDMapping.genius_id, SpotifyArtist.id)
            .where(TrackIDMapping.id == SpotifyTrack.id)
            .where(SpotifyTrack.artist_id == SpotifyArtist.id)
            .where(TrackIDMapping.genius_id.isnot(None))
            .limit(limit)
        )
        cursor = await execute_query(engine=self._db_engine, query=query)
        query_result = cursor.all()

        return {row.genius_id: row.id for row in query_result}
