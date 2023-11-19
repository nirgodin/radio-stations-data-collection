from typing import List, Dict, Coroutine

from postgres_client import ShazamLocation
from shazamio import Shazam

from data_collectors.consts.shazam_consts import ISRAEL_COUNTRY_CODE
from data_collectors.consts.spotify_consts import TRACKS
from data_collectors.contract.collector_interface import ICollector


class ShazamTopTracksCollector(ICollector):
    def __init__(self, shazam: Shazam = Shazam()):
        self._shazam = shazam

    async def collect(self) -> Dict[ShazamLocation, List[dict]]:
        results = {}

        for location, request_method in self._location_to_request_method_mapping.items():
            response = await request_method
            tracks = response[TRACKS]
            results[location] = tracks

        return results

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
