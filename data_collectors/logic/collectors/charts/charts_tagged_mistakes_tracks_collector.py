from typing import Any, List, Set, Dict

from genie_common.tools import logger
from genie_datastores.postgres.models import ChartEntry, SpotifyTrack
from genie_datastores.postgres.operations import execute_query
from spotipyio import SpotifyClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.spotify_consts import TRACK
from data_collectors.contract import ICollector
from data_collectors.logic.models import DBUpdateRequest


class ChartsTaggedMistakesTracksCollector(ICollector):
    def __init__(self, db_engine: AsyncEngine, spotify_client: SpotifyClient):
        self._db_engine = db_engine
        self._spotify_client = spotify_client

    async def collect(
        self, update_requests: List[DBUpdateRequest]
    ) -> List[Dict[str, dict]]:
        ids = self._get_unique_tracks_ids(update_requests)
        non_existing_ids = await self._filter_out_existing_ids(ids)

        if non_existing_ids:
            logger.info(
                "Found non existing tracks ids. Querying Spotify for equivalent tracks records"
            )
            raw_tracks = await self._spotify_client.tracks.info.run(non_existing_ids)
            return [{TRACK: track} for track in raw_tracks]

        logger.info("Did not find any non existing track id. Returning empty list")
        return []

    @staticmethod
    def _get_unique_tracks_ids(update_requests: List[DBUpdateRequest]) -> List[str]:
        tracks_ids = set()

        for request in update_requests:
            request_id = request.values[ChartEntry.track_id]

            if request_id is not None:
                tracks_ids.add(request_id)

        return list(tracks_ids)

    async def _filter_out_existing_ids(self, ids: List[str]) -> List[str]:
        query = select(SpotifyTrack.id).where(SpotifyTrack.id.in_(ids))
        query_result = await execute_query(engine=self._db_engine, query=query)
        existing_ids = query_result.scalars().all()

        return [track_id for track_id in ids if track_id not in existing_ids]
