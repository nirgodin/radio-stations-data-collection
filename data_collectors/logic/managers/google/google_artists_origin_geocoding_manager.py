from typing import Optional, Dict, List

from genie_common.tools import logger, SyncPoolExecutor

from data_collectors.logic.collectors import GoogleGeocodingCollector
from data_collectors.logic.models.geocoding_response import GeocodingResponse
from data_collectors.logic.updaters import ValuesDatabaseUpdater
from data_collectors.contract import IManager
from data_collectors.logic.models import DBUpdateRequest
from data_collectors.logic.serializers import GoogleGeocodingResponseSerializer


class GoogleArtistsOriginGeocodingManager(IManager):
    def __init__(self,
                 geocoding_collector: GoogleGeocodingCollector,
                 db_updater: ValuesDatabaseUpdater,
                 geocoding_serializer: GoogleGeocodingResponseSerializer = GoogleGeocodingResponseSerializer(),
                 pool_executor: SyncPoolExecutor = SyncPoolExecutor()):
        self._geocoding_collector = geocoding_collector
        self._db_updater = db_updater
        self._geocoding_serializer = geocoding_serializer
        self._pool_executor = pool_executor

    async def run(self, limit: Optional[int]) -> None:
        artists_geocoding = await self._geocoding_collector.collect(limit)
        logger.info("Starting to serialize geocoding response to DB structure")
        update_requests = self._pool_executor.run(
            iterable=artists_geocoding,
            func=self._to_update_request,
            expected_type=DBUpdateRequest
        )

        await self._db_updater.update(update_requests)

    def _to_update_request(self, response: GeocodingResponse) -> DBUpdateRequest:
        if response.is_from_cache:
            return DBUpdateRequest(
                id=response.id,
                values=response.result
            )

        serialized_request = self._geocoding_serializer.serialize(response.id, response.result)

        if serialized_request is not None:
            return serialized_request
        else:
            logger.warn(f"Could not serialize geocoding response for artist `{response.id}`. Skipping")
