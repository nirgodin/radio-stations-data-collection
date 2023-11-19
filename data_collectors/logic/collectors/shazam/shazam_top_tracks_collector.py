from typing import List, Dict, Coroutine

from postgres_client import ShazamLocation
from shazamio import Shazam

from data_collectors.consts.shazam_consts import ISRAEL_COUNTRY_CODE
from data_collectors.consts.spotify_consts import TRACKS
from data_collectors.contract.collector_interface import ICollector
from data_collectors.logs import logger


class ShazamTopTracksCollector(ICollector):
    def __init__(self, shazam: Shazam = Shazam()):
        self._shazam = shazam

    async def collect(self) -> Dict[ShazamLocation, List[dict]]:
        logger.info("Starting to execute shazam top tracks collection")
        results = {}

        for location, request_method in self._location_to_request_method_mapping.items():
            tracks = await self._collect_single_location_tracks(location, request_method)
            results[location] = tracks

        return results

    @staticmethod
    async def _collect_single_location_tracks(location: ShazamLocation, request_method: Coroutine) -> List[dict]:
        logger.info(f"Starting to collect tracks for `{location.value}`")
        response = await request_method

        return response[TRACKS]

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
