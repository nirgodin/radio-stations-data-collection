from datetime import datetime
from typing import List

from genie_common.tools import logger, AioPoolExecutor
from genie_datastores.postgres.models import Artist
from genie_datastores.postgres.utils import update_by_values
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IDatabaseUpdater
from data_collectors.logic.models import DBUpdateRequest


class ArtistsDatabaseUpdater(IDatabaseUpdater):
    def __init__(self, db_engine: AsyncEngine, pool_executor: AioPoolExecutor):
        super().__init__(db_engine)
        self._pool_executor = pool_executor

    async def update(self, update_requests: List[DBUpdateRequest]) -> None:
        n_artists = len(update_requests)
        logger.info(f"Starting to update records for {n_artists}")
        results = await self._pool_executor.run(  # TODO: Find a way to do it in Bulk
            iterable=update_requests,
            func=self._update_single_artist,
            expected_type=type(None)
        )

        logger.info(f"Successfully updated {len(results)} records out of {n_artists}")

    async def _update_single_artist(self, update_request: DBUpdateRequest) -> None:
        update_request.values[Artist.update_date] = datetime.now()
        await update_by_values(
            self._db_engine,
            Artist,
            update_request.values,
            Artist.id == update_request.id,
        )
