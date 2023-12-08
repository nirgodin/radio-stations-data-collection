from typing import List

from genie_common.tools import logger, AioPoolExecutor
from genie_datastores.postgres.models import SpotifyArtist
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import BaseDatabaseUpdater
from data_collectors.logic.models import DBUpdateRequest


class SpotifyArtistsDatabaseUpdater(BaseDatabaseUpdater):
    def __init__(self, db_engine: AsyncEngine, pool_executor: AioPoolExecutor):
        super().__init__(db_engine)
        self._pool_executor = pool_executor

    async def update(self, update_requests: List[DBUpdateRequest]) -> None:
        n_artists = len(update_requests)
        logger.info(f"Starting to update spotify artists genders for {n_artists} records")
        results = await self._pool_executor.run(  # TODO: Find a way to do it in Bulk
            iterable=update_requests,
            func=self._update_single_artist_gender,
            expected_type=type(None)
        )

        logger.info(f"Successfully updated {len(results)} artists genders out of {n_artists} using spotify images")

    async def _update_single_artist_gender(self, update_request: DBUpdateRequest) -> None:
        await self._update_by_values(
            SpotifyArtist,
            update_request.values,
            SpotifyArtist.id == update_request.id
        )
