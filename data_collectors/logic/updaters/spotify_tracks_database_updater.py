from typing import List

from genie_common.tools import logger, AioPoolExecutor
from genie_datastores.postgres.models import SpotifyTrack
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import BaseDatabaseUpdater
from data_collectors.logic.models import DBUpdateRequest


class SpotifyTracksDatabaseUpdater(BaseDatabaseUpdater):
    def __init__(self, db_engine: AsyncEngine, pool_executor: AioPoolExecutor):
        super().__init__(db_engine)
        self._pool_executor = pool_executor

    async def update(self, update_requests: List[DBUpdateRequest]) -> None:
        n_tracks = len(update_requests)
        logger.info(f"Starting to update {n_tracks} spotify tracks records")
        results = await self._pool_executor.run(
            iterable=update_requests,
            func=self._update_single_track,
            expected_type=type(None)
        )

        logger.info(f"Successfully updated {len(results)} artists genders out of {n_tracks} using spotify images")

    async def _update_single_track(self, update_request: DBUpdateRequest) -> None:
        await self._update_by_values(
            SpotifyTrack,
            update_request.values,
            SpotifyTrack.id == update_request.id
        )
