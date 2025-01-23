from http import HTTPStatus
from typing import Dict, List

from _pytest.fixtures import fixture
from genie_datastores.postgres.models import SpotifyStation
from spotipyio.testing import SpotifyTestClient, SpotifyMockFactory
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.jobs.job_id import JobId
from tests.helpers.spotify_playlists_resources import SpotifyPlaylistsResources
from tests.tools.playlists_resources_creator import PlaylistsResourcesCreator


class TestSpotifyChartsManager:
    async def test_trigger(
        self,
        spotify_test_client: SpotifyTestClient,
        test_client: TestClient,
        playlist_resources_map: Dict[str, SpotifyPlaylistsResources],
        db_engine: AsyncEngine,
    ):
        self._given_valid_playlists_responses(
            playlist_resources_map, spotify_test_client
        )
        self._given_valid_tracks_responses(playlist_resources_map, spotify_test_client)

        with test_client as client:
            actual = client.post(f"jobs/trigger/{JobId.SPOTIFY_CHARTS.value}")

        assert actual.status_code == HTTPStatus.OK

    @staticmethod
    def _given_valid_playlists_responses(
        playlist_resources_map: Dict[str, SpotifyPlaylistsResources],
        spotify_test_client: SpotifyTestClient,
    ):
        for playlist_id, playlist_resources in playlist_resources_map.items():
            spotify_test_client.playlists.info.expect_success(
                playlist_id, [playlist_resources.playlist]
            )

    @fixture
    def playlist_resources_map(self) -> Dict[str, SpotifyPlaylistsResources]:
        playlists = [
            "37i9dQZEVXbJ6IpvItkve3",
            "37i9dQZEVXbMDoHDwVN2tF",
        ]  # TODO: Move to const

        return PlaylistsResourcesCreator.create(playlists)

    @fixture
    def tracks(self, playlist_resources_map) -> List[str]:
        tracks_ids = []

        for playlist in playlist_resources_map.values():
            playlist_tracks = playlist["tracks"]["items"]
            playlist_tracks_ids = [track["track"]["id"] for track in playlist_tracks]
            tracks_ids.extend(playlist_tracks_ids)

        return tracks_ids

    @staticmethod
    def _given_valid_tracks_responses(
        playlist_resources_map: Dict[str, SpotifyPlaylistsResources],
        spotify_test_client: SpotifyTestClient,
    ):
        for resources in playlist_resources_map.values():
            for track_id, track in resources.tracks.items():
                spotify_test_client.tracks.info.expect_success(
                    [track_id], responses_json=[track]
                )
