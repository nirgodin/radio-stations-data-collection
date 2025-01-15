from http import HTTPStatus
from typing import Dict, List

from _pytest.fixtures import fixture
from aioresponses import aioresponses
from genie_common.utils import random_alphanumeric_string, chain_lists
from genie_datastores.postgres.models import ShazamLocation
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.consts.shazam_consts import DATA
from data_collectors.consts.spotify_consts import ID
from data_collectors.jobs.job_id import JobId
from tests.helpers.shazam_track_resources import ShazamTrackResources

# Must have the space in the end of URL in order aioresponses matches the request
SHAZAM_TRACK_URL_FORMAT = "https://www.shazam.com/discovery/v5/EN/GB/web/-/track/{id}?shazamapiversion=v3&video=v3 "
SHAZAM_ARTIST_URL_FORMAT = "https://www.shazam.com/services/amapi/v1/catalog/GB/artists/{id}?extend=artistBio,bornOrFormed,editorialArtwork,origin&views=full-albums,featured-albums,latest-release,top-music-videos,similar-artists,top-songs,playlists"


class TestShazamTopTracksManager:
    async def test_trigger(
        self,
        test_client: TestClient,
        mock_responses: aioresponses,
        location_playlist_id_map: Dict[str, str],
        location_shazam_tracks_map: Dict[str, List[ShazamTrackResources]],
        db_engine: AsyncEngine,
    ) -> None:
        self._given_successful_top_tracks_requests(
            mock_responses=mock_responses,
            location_playlist_id_map=location_playlist_id_map,
            location_shazam_tracks_map=location_shazam_tracks_map,
        )
        self._given_successful_tracks_requests(
            mock_responses, location_shazam_tracks_map
        )
        self._given_successful_artists_requests(
            mock_responses, location_shazam_tracks_map
        )

        with test_client as client:
            actual = client.post(f"/jobs/trigger/{JobId.SHAZAM_TOP_TRACKS.value}")

        assert actual.status_code == HTTPStatus.OK.value

    def _given_successful_top_tracks_requests(
        self,
        mock_responses: aioresponses,
        location_playlist_id_map: Dict[str, str],
        location_shazam_tracks_map: Dict[str, List[ShazamTrackResources]],
    ) -> None:
        self._given_successful_geo_responses(mock_responses, location_playlist_id_map)

        for location, playlist_id in location_playlist_id_map.items():
            tracks_ids = [
                {ID: str(track.id)} for track in location_shazam_tracks_map[location]
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
    def location_shazam_tracks_map(
        self, locations: List[ShazamLocation]
    ) -> Dict[str, List[ShazamTrackResources]]:
        return {
            location.value: self._random_shazam_resources() for location in locations
        }

    @fixture
    def locations(self) -> List[ShazamLocation]:
        return [ShazamLocation.TEL_AVIV, ShazamLocation.ISRAEL, ShazamLocation.WORLD]

    @staticmethod
    def _random_shazam_resources() -> List[ShazamTrackResources]:
        return [ShazamTrackResources.random() for _ in range(200)]

    @staticmethod
    def _given_successful_tracks_requests(
        mock_responses: aioresponses,
        location_shazam_tracks_map: Dict[str, List[ShazamTrackResources]],
    ) -> None:
        tracks = chain_lists(list(location_shazam_tracks_map.values()))

        for track in tracks:
            mock_responses.get(
                url=SHAZAM_TRACK_URL_FORMAT.format(id=track.id),
                payload=track.to_track_response(),
            )

    @staticmethod
    def _given_successful_artists_requests(
        mock_responses: aioresponses,
        location_shazam_tracks_map: Dict[str, List[ShazamTrackResources]],
    ) -> None:
        tracks = chain_lists(list(location_shazam_tracks_map.values()))

        for track in tracks:
            mock_responses.get(
                url=SHAZAM_ARTIST_URL_FORMAT.format(id=track.artist_id),
                payload=track.to_artist_response(),
            )
