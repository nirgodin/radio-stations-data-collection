from typing import Optional, List, Dict

from aiohttp import ClientSession
from genie_common.tools import AioPoolExecutor, logger
from genie_common.utils import jsonify_response, merge_dicts
from genie_datastores.postgres.models import ShazamArtist, Artist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.google_consts import GOOGLE_GEOCODING_API_URL, ADDRESS, LANGUAGE
from data_collectors.contract import ICollector


class GoogleGeocodingCollector(ICollector):
    def __init__(self, db_engine: AsyncEngine, pool_executor: AioPoolExecutor, session: ClientSession):
        self._db_engine = db_engine
        self._pool_executor = pool_executor
        self._session = session

    async def collect(self, limit: Optional[int]) -> Dict[str, dict]:
        missing_location_rows = await self._query_missing_geocoding_locations(limit)

        if not missing_location_rows:
            logger.info("Did not find any artist with missing origin location. Returning empty dict.")
            return {}

        logger.info(f"Successfully retrieved {len(missing_location_rows)} missing artists origin locations")
        logger.info(f"Starting to geocode {len(missing_location_rows)} locations using Google Geocode API")
        results = await self._pool_executor.run(
            iterable=missing_location_rows,
            func=self._geocode_single_location,
            expected_type=dict
        )

        return merge_dicts(*results)

    async def _query_missing_geocoding_locations(self, limit: Optional[int]) -> List[Row]:
        logger.info("Starting to query artists origin locations based on `shazam_artists` table data")
        query = (
            select(Artist.id, ShazamArtist.origin)
            .where(Artist.shazam_id == ShazamArtist.id)
            .where(Artist.country.is_(None))
            .where(ShazamArtist.origin.isnot(None))
            .order_by(Artist.update_date.desc())
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.all()

    async def _geocode_single_location(self, row: Row) -> Dict[str, dict]:
        params = {
            ADDRESS: row.origin,
            LANGUAGE: "en"
        }

        async with self._session.get(url=GOOGLE_GEOCODING_API_URL, params=params) as raw_response:
            response = await jsonify_response(raw_response)

        return {row.id: response}
