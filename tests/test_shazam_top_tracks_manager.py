from functools import partial
from http import HTTPStatus
from typing import Dict, List

from _pytest.fixtures import fixture
from aioresponses import aioresponses
from genie_common.utils import random_alphanumeric_string, chain_lists
from genie_datastores.postgres.models import ShazamLocation, ShazamTopTrack
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.components import ComponentFactory
from data_collectors.consts.shazam_consts import DATA
from data_collectors.consts.spotify_consts import ID
from data_collectors.jobs.job_builders.shazam_top_tracks_job_builder import (
    ShazamTopTracksJobBuilder,
)
from data_collectors.jobs.job_id import JobId
from tests.helpers.shazam_track_resources import ShazamTrackResources
from tests.testing_utils import until, build_scheduled_test_client
from tests.tools.shazam_insertions_verifier import ShazamInsertionsVerifier

# Must have the space in the end of URL in order aioresponses matches the request
SHAZAM_TRACK_URL_FORMAT = "https://www.shazam.com/discovery/v5/EN/GB/web/-/track/{id}?shazamapiversion=v3&video=v3 "
SHAZAM_ARTIST_URL_FORMAT = "https://www.shazam.com/services/amapi/v1/catalog/GB/artists/{id}?extend=artistBio,bornOrFormed,editorialArtwork,origin&views=full-albums,featured-albums,latest-release,top-music-videos,similar-artists,top-songs,playlists"


class TestShazamTopTracksManager:
    async def test_trigger(
        self,
        test_client: TestClient,
        mock_aioresponses: aioresponses,
        location_playlist_id_map: Dict[ShazamLocation, str],
        location_shazam_tracks_map: Dict[ShazamLocation, List[ShazamTrackResources]],
        shazam_tracks: List[ShazamTrackResources],
        unique_shazam_tracks: List[ShazamTrackResources],
        db_engine: AsyncEngine,
        shazam_insertions_verifier: ShazamInsertionsVerifier,
    ) -> None:
        self._given_expected_shazam_responses(
            mock_responses=mock_aioresponses,
            location_playlist_id_map=location_playlist_id_map,
            location_shazam_tracks_map=location_shazam_tracks_map,
            shazam_tracks=shazam_tracks,
        )

        with test_client as client:
            actual = client.post(f"/jobs/trigger/{JobId.SHAZAM_TOP_TRACKS.value}")

        assert actual.status_code == HTTPStatus.OK.value
        assert await self._are_expected_db_records_inserted(
            shazam_insertions_verifier=shazam_insertions_verifier,
            unique_shazam_tracks=unique_shazam_tracks,
            db_engine=db_engine,
            location_shazam_tracks_map=location_shazam_tracks_map,
        )

    async def test_scheduled_job(
        self,
        scheduled_test_client: TestClient,
        mock_aioresponses: aioresponses,
        location_playlist_id_map: Dict[ShazamLocation, str],
        location_shazam_tracks_map: Dict[ShazamLocation, List[ShazamTrackResources]],
        shazam_tracks: List[ShazamTrackResources],
        unique_shazam_tracks: List[ShazamTrackResources],
        db_engine: AsyncEngine,
        shazam_insertions_verifier: ShazamInsertionsVerifier,
    ):
        self._given_expected_shazam_responses(
            mock_responses=mock_aioresponses,
            location_playlist_id_map=location_playlist_id_map,
            location_shazam_tracks_map=location_shazam_tracks_map,
            shazam_tracks=shazam_tracks,
        )
        condition = partial(
            self._are_expected_db_records_inserted,
            shazam_insertions_verifier=shazam_insertions_verifier,
            unique_shazam_tracks=unique_shazam_tracks,
            db_engine=db_engine,
            location_shazam_tracks_map=location_shazam_tracks_map,
        )

        with scheduled_test_client:
            await until(condition)

    def _given_successful_top_tracks_requests(
        self,
        mock_responses: aioresponses,
        location_playlist_id_map: Dict[ShazamLocation, str],
        location_shazam_tracks_map: Dict[ShazamLocation, List[ShazamTrackResources]],
    ) -> None:
        self._given_successful_geo_responses(mock_responses, location_playlist_id_map)

        for location, playlist_id in location_playlist_id_map.items():
            tracks_ids = [{ID: str(track.id)} for track in location_shazam_tracks_map[location]]
            mock_responses.get(
                url=f"https://www.shazam.com/services/amapi/v1/catalog/GB/playlists/{playlist_id}/tracks?limit=200&offset=0&l=EN&relate[songs]=artists,music-videos",
                payload={DATA: tracks_ids},
            )

    @staticmethod
    def _given_successful_geo_responses(
        mock_responses: aioresponses,
        location_playlist_id_map: Dict[ShazamLocation, str],
    ) -> None:
        payload = {
            "countries": [
                {
                    "id": "IL",
                    "listid": location_playlist_id_map[ShazamLocation.ISRAEL],
                    "cities": [
                        {
                            "name": ShazamLocation.TEL_AVIV.value,
                            "listid": location_playlist_id_map[ShazamLocation.TEL_AVIV],
                        }
                    ],
                }
            ],
            "global": {
                "top": {"listid": location_playlist_id_map[ShazamLocation.WORLD]},
            },
        }
        mock_responses.get(
            url="https://www.shazam.com/services/charts/locations",
            payload=payload,
            repeat=len(location_playlist_id_map),
        )

    @fixture
    def location_playlist_id_map(self, locations: List[ShazamLocation]) -> Dict[ShazamLocation, str]:
        return {location: random_alphanumeric_string() for location in locations}

    @fixture
    def location_shazam_tracks_map(
        self, locations: List[ShazamLocation]
    ) -> Dict[ShazamLocation, List[ShazamTrackResources]]:
        return {location: self._random_shazam_resources() for location in locations}

    @fixture
    def locations(self) -> List[ShazamLocation]:
        return [ShazamLocation.TEL_AVIV, ShazamLocation.ISRAEL, ShazamLocation.WORLD]

    @fixture
    def shazam_tracks(
        self, location_shazam_tracks_map: Dict[str, List[ShazamTrackResources]]
    ) -> List[ShazamTrackResources]:
        return chain_lists(list(location_shazam_tracks_map.values()))

    @fixture
    def unique_shazam_tracks(self, shazam_tracks: List[ShazamTrackResources]) -> List[ShazamTrackResources]:
        seen = set()
        unique = []

        for track in shazam_tracks:
            if track.id not in seen:
                unique.append(track)
                seen.add(track.id)

        return unique

    @fixture
    async def scheduled_test_client(
        self,
        component_factory: ComponentFactory,
    ) -> TestClient:
        scheduled_client = await build_scheduled_test_client(component_factory, ShazamTopTracksJobBuilder)
        with scheduled_client as client:
            yield client

    @staticmethod
    def _random_shazam_resources() -> List[ShazamTrackResources]:
        return [ShazamTrackResources.random() for _ in range(200)]

    @staticmethod
    def _given_successful_tracks_requests(
        mock_responses: aioresponses, shazam_tracks: List[ShazamTrackResources]
    ) -> None:
        for track in shazam_tracks:
            mock_responses.get(
                url=SHAZAM_TRACK_URL_FORMAT.format(id=track.id),
                payload=track.to_track_response(),
            )

    @staticmethod
    def _given_successful_artists_requests(
        mock_responses: aioresponses, shazam_tracks: List[ShazamTrackResources]
    ) -> None:
        for track in shazam_tracks:
            mock_responses.get(
                url=SHAZAM_ARTIST_URL_FORMAT.format(id=track.artist_id),
                payload=track.to_artist_response(),
            )

    def _given_expected_shazam_responses(
        self,
        mock_responses: aioresponses,
        location_playlist_id_map: Dict[ShazamLocation, str],
        location_shazam_tracks_map: Dict[ShazamLocation, List[ShazamTrackResources]],
        shazam_tracks: List[ShazamTrackResources],
    ) -> None:
        self._given_successful_top_tracks_requests(
            mock_responses=mock_responses,
            location_playlist_id_map=location_playlist_id_map,
            location_shazam_tracks_map=location_shazam_tracks_map,
        )
        self._given_successful_tracks_requests(mock_responses, shazam_tracks)
        self._given_successful_artists_requests(mock_responses, shazam_tracks)

    async def _are_expected_db_records_inserted(
        self,
        shazam_insertions_verifier: ShazamInsertionsVerifier,
        unique_shazam_tracks: List[ShazamTrackResources],
        db_engine: AsyncEngine,
        location_shazam_tracks_map: Dict[ShazamLocation, List[ShazamTrackResources]],
    ) -> bool:
        inserted_expected_shazam_tracks = await shazam_insertions_verifier.verify(
            tracks={str(track.id) for track in unique_shazam_tracks},
            artists={str(track.artist_id) for track in unique_shazam_tracks},
        )

        if inserted_expected_shazam_tracks:
            return await self._are_expected_top_tracks_records_inserted(db_engine, location_shazam_tracks_map)

        return False

    @staticmethod
    async def _are_expected_top_tracks_records_inserted(
        db_engine: AsyncEngine,
        location_shazam_tracks_map: Dict[ShazamLocation, List[ShazamTrackResources]],
    ) -> bool:
        for location, tracks in location_shazam_tracks_map.items():
            expected = [str(track.id) for track in tracks]
            query = select(ShazamTopTrack.track_id).where(ShazamTopTrack.location == location)
            query_result = await execute_query(db_engine, query)
            actual = query_result.scalars().all()

            if sorted(actual) != sorted(expected):
                return False

        return True
