from typing import List, Dict, Coroutine, Tuple

from postgres_client import ShazamLocation

from data_collectors.consts.shazam_consts import ISRAEL_COUNTRY_CODE
from data_collectors.consts.spotify_consts import TRACKS
from data_collectors.logic.collectors.shazam.base_shazam_collector import BaseShazamCollector
from data_collectors.logic.utils import merge_dicts
from data_collectors.logs import logger


class ShazamTopTracksCollector(BaseShazamCollector):
    async def collect(self) -> Dict[ShazamLocation, List[dict]]:
        logger.info("Starting to execute shazam top tracks collection")
        results = await self._pool_executor.run(
            iterable=self._location_to_request_method_mapping.items(),
            func=self._collect_single_location_tracks,
            expected_type=dict
        )
        logger.info("Successfully finished executing shazam top tracks collector")

        return merge_dicts(*results)

    @staticmethod
    async def _collect_single_location_tracks(location_and_request_method: Tuple[ShazamLocation, Coroutine]) -> Dict[ShazamLocation, List[dict]]:
        location, request_method = location_and_request_method
        logger.info(f"Starting to collect tracks for `{location.value}`")
        response = await request_method

        return {location: response[TRACKS]}

    @property
    def _location_to_request_method_mapping(self) -> Dict[ShazamLocation, Coroutine]:
        return {
            ShazamLocation.BEER_SHEBA: self._shazam.top_city_tracks(
                country_code=ISRAEL_COUNTRY_CODE,
                city_name=ShazamLocation.BEER_SHEBA.value,
                limit=200
            ),
            ShazamLocation.HAIFA: self._shazam.top_city_tracks(
                country_code=ISRAEL_COUNTRY_CODE,
                city_name=ShazamLocation.HAIFA.value,
                limit=200
            ),
            ShazamLocation.JERUSALEM: self._shazam.top_city_tracks(
                country_code=ISRAEL_COUNTRY_CODE,
                city_name=ShazamLocation.JERUSALEM.value,
                limit=200
            ),
            ShazamLocation.NETANYA: self._shazam.top_city_tracks(
                country_code=ISRAEL_COUNTRY_CODE,
                city_name=ShazamLocation.NETANYA.value,
                limit=200
            ),
            ShazamLocation.TEL_AVIV: self._shazam.top_city_tracks(
                country_code=ISRAEL_COUNTRY_CODE,
                city_name=ShazamLocation.TEL_AVIV.value,
                limit=200
            ),
            ShazamLocation.ISRAEL: self._shazam.top_country_tracks(
                country_code=ISRAEL_COUNTRY_CODE,
                limit=200
            ),
            ShazamLocation.WORLD: self._shazam.top_world_tracks(
                limit=200
            )
        }
