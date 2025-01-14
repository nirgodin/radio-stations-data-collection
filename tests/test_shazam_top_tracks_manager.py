from random import randint
from typing import Dict, List

from _pytest.fixtures import fixture
from aioresponses import aioresponses
from genie_common.utils import random_alphanumeric_string, chain_lists
from genie_datastores.postgres.models import ShazamLocation
from starlette.testclient import TestClient

from data_collectors.consts.shazam_consts import DATA, ADAM_ID, KEY, TITLE
from data_collectors.consts.spotify_consts import ID, ARTISTS
from data_collectors.jobs.job_id import JobId

# Must have the space in the end of URL in order aioresponses matches the request
SHAZAM_TRACK_URL_FORMAT = "https://www.shazam.com/discovery/v5/EN/GB/web/-/track/{id}?shazamapiversion=v3&video=v3 "


class TestShazamTopTracksManager:
    async def test_trigger(
        self,
        test_client: TestClient,
        mock_responses: aioresponses,
        location_playlist_id_map: Dict[str, str],
        location_tracks_ids_map: Dict[str, List[int]],
    ) -> None:
        self._given_successful_top_tracks_requests(
            mock_responses=mock_responses,
            location_playlist_id_map=location_playlist_id_map,
            location_tracks_ids_map=location_tracks_ids_map,
        )
        self._given_successful_tracks_requests(mock_responses, location_tracks_ids_map)
        with test_client as client:
            actual = client.post(f"/jobs/trigger/{JobId.SHAZAM_TOP_TRACKS.value}")

    def _given_successful_top_tracks_requests(
        self,
        mock_responses: aioresponses,
        location_playlist_id_map: Dict[str, str],
        location_tracks_ids_map: Dict[str, List[int]],
    ) -> None:
        self._given_successful_geo_responses(mock_responses, location_playlist_id_map)

        for location, playlist_id in location_playlist_id_map.items():
            tracks_ids = [
                {ID: track_id} for track_id in location_tracks_ids_map[location]
            ]
            mock_responses.get(
                url=f"https://www.shazam.com/services/amapi/v1/catalog/GB/playlists/{playlist_id}/tracks?limit=200&offset=0&l=EN&relate[songs]=artists,music-videos",
                payload={DATA: tracks_ids},
            )

    @staticmethod
    def _given_successful_geo_responses(
        mock_responses: aioresponses, location_playlist_id_map: Dict[str, str]
    ) -> None:
        payload = {
            "countries": [
                {
                    "id": "IL",
                    "listid": location_playlist_id_map[ShazamLocation.ISRAEL.value],
                    "cities": [
                        {
                            "name": ShazamLocation.TEL_AVIV.value,
                            "listid": location_playlist_id_map[
                                ShazamLocation.TEL_AVIV.value
                            ],
                        }
                    ],
                }
            ],
            "global": {
                "top": {"listid": location_playlist_id_map[ShazamLocation.WORLD.value]},
            },
        }
        mock_responses.get(
            url="https://www.shazam.com/services/charts/locations",
            payload=payload,
            repeat=len(location_playlist_id_map),
        )

    @fixture
    def location_playlist_id_map(
        self, locations: List[ShazamLocation]
    ) -> Dict[str, str]:
        return {location.value: random_alphanumeric_string() for location in locations}

    @fixture
    def location_tracks_ids_map(
        self, locations: List[ShazamLocation]
    ) -> Dict[str, List[int]]:
        return {location.value: self._random_tracks_ids() for location in locations}

    @fixture
    def locations(self) -> List[ShazamLocation]:
        return [ShazamLocation.TEL_AVIV, ShazamLocation.ISRAEL, ShazamLocation.WORLD]

    @staticmethod
    def _random_tracks_ids() -> List[int]:
        return [randint(1, 50000) for _ in range(200)]

    @staticmethod
    def _given_successful_tracks_requests(
        mock_responses: aioresponses, location_tracks_ids_map: Dict[str, List[int]]
    ) -> None:
        # 'https://www.shazam.com/discovery/v5/EN/GB/web/-/track/30721?shazamapiversion=v3&video=v3 '
        track_ids = chain_lists(list(location_tracks_ids_map.values()))

        for track_id in track_ids:
            mock_responses.get(
                url=SHAZAM_TRACK_URL_FORMAT.format(id=track_id),
                payload={
                    KEY: track_id,
                    TITLE: random_alphanumeric_string(),
                    ARTISTS: [{ADAM_ID: randint(1, 50000)}],
                },
            )
