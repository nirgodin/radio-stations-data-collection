from typing import Optional, Dict, List

from genie_common.tools import logger

from data_collectors.logic.collectors import GoogleGeocodingCollector
from data_collectors.logic.updaters import ValuesDatabaseUpdater
from data_collectors.contract import IManager
from data_collectors.logic.models import DBUpdateRequest
from data_collectors.logic.serializers import GoogleGeocodingResponseSerializer


class GoogleArtistsOriginGeocodingManager(IManager):
    def __init__(self,
                 geocoding_collector: GoogleGeocodingCollector,
                 db_updater: ValuesDatabaseUpdater,
                 geocoding_serializer: GoogleGeocodingResponseSerializer = GoogleGeocodingResponseSerializer()):
        self._geocoding_collector = geocoding_collector
        self._db_updater = db_updater
        self._geocoding_serializer = geocoding_serializer

    async def run(self, limit: Optional[int]) -> None:
        artists_geocoding = await self._geocoding_collector.collect(limit)
        logger.info("Starting to serialize geocoding response to DB structure")
        update_requests = self._to_update_requests(artists_geocoding)

        await self._db_updater.update(update_requests)

    def _to_update_requests(self, artists_geocoding: Dict[str, dict]) -> List[DBUpdateRequest]:
        update_requests = []

        for artist_id, geocoding in artists_geocoding.items():
            serialized_request = self._geocoding_serializer.serialize(artist_id, geocoding)

            if serialized_request is not None:
                update_requests.append(serialized_request)
            else:
                logger.warn(f"Could not serialize geocoding response for artist `{artist_id}`. Skipping")

        return update_requests
