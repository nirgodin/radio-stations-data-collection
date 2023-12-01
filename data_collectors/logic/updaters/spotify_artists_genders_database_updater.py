from typing import Optional, Dict, Tuple

from genie_common.tools import logger, AioPoolExecutor
from genie_datastores.postgres.models import SpotifyArtist, Gender, DataSource
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import BaseDatabaseUpdater


class SpotifyArtistsGendersDatabaseUpdater(BaseDatabaseUpdater):
    def __init__(self, db_engine: AsyncEngine, pool_executor: AioPoolExecutor):
        super().__init__(db_engine)
        self._pool_executor = pool_executor

    async def update(self, ids_mapping: Dict[str, Optional[Gender]], value_column: SpotifyArtist) -> None:
        n_artists = len(ids_mapping)
        logger.info(f"Starting to update spotify artists genders for {n_artists} records")
        results = await self._pool_executor.run(  # TODO: Find a way to do it in Bulk
            iterable=ids_mapping.items(),
            func=self._update_single_artist_gender,
            expected_type=type(None)
        )

        logger.info(f"Successfully updated {len(results)} artists genders out of {n_artists} using spotify images")

    async def _update_single_artist_gender(self, artist_id_and_gender: Tuple[str, Optional[Gender]]) -> None:
        artist_id, artist_gender = artist_id_and_gender
        values = {
            SpotifyArtist.gender: artist_gender,
            SpotifyArtist.gender_source: DataSource.SPOTIFY_IMAGES
        }
        await self._update_by_values(SpotifyArtist, values, SpotifyArtist.id == artist_id)
