from typing import List

from genie_common.tools import logger, AioPoolExecutor
from genie_datastores.postgres.models import Artist
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import BaseDatabaseUpdater
from data_collectors.logic.models import DBUpdateRequest


class ArtistsDatabaseUpdater(BaseDatabaseUpdater):
    def __init__(self, db_engine: AsyncEngine, pool_executor: AioPoolExecutor):
        super().__init__(db_engine)
        self._pool_executor = pool_executor

    async def update(self, update_requests: List[DBUpdateRequest]) -> None:
        n_artists = len(update_requests)
        logger.info(f"Starting to update artists records for {n_artists}")
        results = await self._pool_executor.run(  # TODO: Find a way to do it in Bulk
            iterable=update_requests,
            func=self._update_single_artist,
            expected_type=type(None)
        )

        logger.info(f"Successfully updated {len(results)} artists records out of {n_artists}")

    async def _update_single_artist(self, update_request: DBUpdateRequest) -> None:
        await self._update_by_values(
            Artist,
            update_request.values,
            Artist.id == update_request.id
        )
