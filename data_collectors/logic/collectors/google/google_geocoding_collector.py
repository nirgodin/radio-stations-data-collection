from typing import Optional, List

from aiohttp import ClientSession
from async_lru import alru_cache
from genie_common.tools import AioPoolExecutor, logger
from genie_common.clients.utils import jsonify_response
from genie_datastores.postgres.models import Artist
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.google_consts import (
    GOOGLE_GEOCODING_API_URL,
    ADDRESS,
    LANGUAGE,
)
from data_collectors.contract import ICollector
from data_collectors.logic.models.geocoding_response import GeocodingResponse

ARTIST_TABLE_GEOCODING_COLUMNS = [
    Artist.origin,
    Artist.country,
    Artist.state,
    Artist.county,
    Artist.city,
    Artist.latitude,
    Artist.longitude,
]


class GoogleGeocodingCollector(ICollector):
    def __init__(
        self,
        db_engine: AsyncEngine,
        pool_executor: AioPoolExecutor,
        session: ClientSession,
    ):
        self._db_engine = db_engine
        self._pool_executor = pool_executor
        self._session = session

    async def collect(self, limit: Optional[int]) -> List[GeocodingResponse]:
        missing_location_rows = await self._query_missing_geocoding_locations(limit)

        if not missing_location_rows:
            logger.info(
                "Did not find any artist with missing origin location. Returning empty list."
            )
            return []

        logger.info(
            f"Starting to geocode {len(missing_location_rows)} locations using Google Geocode API"
        )
        results = await self._pool_executor.run(
            iterable=missing_location_rows,
            func=self._geocode_single_location,
            expected_type=GeocodingResponse,
        )
        self._log_results(results)

        return results

    async def _query_missing_geocoding_locations(
        self, limit: Optional[int]
    ) -> List[Row]:
        logger.info(
            "Starting to query artists origin locations based on `artists` table data"
        )
        query = (
            select(Artist.id, Artist.origin)
            .where(Artist.country.is_(None))
            .where(Artist.origin.isnot(None))
            .order_by(Artist.update_date.asc())
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.all()

    async def _geocode_single_location(self, row: Row) -> GeocodingResponse:
        existing_location_geocoding = await self._query_db_for_existing_location(row)

        if existing_location_geocoding is None:
            return await self._send_geocoding_request(row)

        return existing_location_geocoding

    @alru_cache
    async def _query_db_for_existing_location(
        self, row: Row
    ) -> Optional[GeocodingResponse]:
        query = (
            select(*ARTIST_TABLE_GEOCODING_COLUMNS)
            .where(Artist.origin == row.origin)
            .where(Artist.country.isnot(None))
            .limit(1)
        )
        response = await execute_query(engine=self._db_engine, query=query)
        query_result = response.first()

        if query_result is not None:
            result = {
                col: getattr(query_result, col.key)
                for col in ARTIST_TABLE_GEOCODING_COLUMNS
            }
            return GeocodingResponse(id=row.id, result=result, is_from_cache=True)

    @alru_cache
    async def _send_geocoding_request(self, row: Row) -> GeocodingResponse:
        params = {ADDRESS: row.origin, LANGUAGE: "en"}

        async with self._session.get(
            url=GOOGLE_GEOCODING_API_URL, params=params
        ) as raw_response:
            response = await jsonify_response(raw_response)

        return GeocodingResponse(id=row.id, result=response, is_from_cache=False)

    @staticmethod
    def _log_results(results: List[GeocodingResponse]) -> None:
        results_number = len(results)
        cache_hits = [result for result in results if result.is_from_cache]
        cache_hits_number = len(cache_hits)

        logger.info(
            f"Out of {results_number} results, {cache_hits_number} were cache hits"
        )
