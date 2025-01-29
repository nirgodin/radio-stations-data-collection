from http import HTTPStatus
from typing import Dict, List

from _pytest.fixtures import fixture
from genie_common.utils import chain_lists
from spotipyio.testing import SpotifyTestClient
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.jobs.job_id import JobId
from tests.helpers.spotify_playlists_resources import SpotifyPlaylistsResources
from tests.tools.playlists_resources_creator import PlaylistsResourcesCreator
from tests.tools.spotify_insertions_verifier import SpotifyInsertionsVerifier


class TestSpotifyChartsManager:
    async def test_trigger(
        self,
        spotify_test_client: SpotifyTestClient,
        test_client: TestClient,
        playlist_resources_map: Dict[str, SpotifyPlaylistsResources],
        spotify_insertions_verifier: SpotifyInsertionsVerifier,
        db_engine: AsyncEngine,
    ):
        self._given_valid_playlists_responses(
            playlist_resources_map, spotify_test_client
        )
        self._given_valid_tracks_responses(playlist_resources_map, spotify_test_client)
        self._given_valid_artists_responses(spotify_test_client, playlist_resources_map)
        self._given_valid_audio_features_responses(
            spotify_test_client, playlist_resources_map
        )

        with test_client as client:
            actual = client.post(f"jobs/trigger/{JobId.SPOTIFY_CHARTS.value}")

        assert actual.status_code == HTTPStatus.OK
        assert await self._are_expected_db_records_inserted(
            db_engine, spotify_insertions_verifier, playlist_resources_map
        )

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

    @staticmethod
    def _given_valid_tracks_responses(
        playlist_resources_map: Dict[str, SpotifyPlaylistsResources],
        spotify_test_client: SpotifyTestClient,
    ):
        for resources in playlist_resources_map.values():
            for track_id, track in resources.tracks.items():
                spotify_test_client.tracks.info.expect_success(
                    [track_id], responses_json=[{"tracks": [track["track"]]}]
                )

    @staticmethod
    def _given_valid_artists_responses(
        spotify_test_client: SpotifyTestClient,
        playlists_resources_map: Dict[str, SpotifyPlaylistsResources],
    ) -> None:
        artists_ids = chain_lists(
            [resource.artist_ids for resource in playlists_resources_map.values()]
        )
        spotify_test_client.artists.info.expect_success(sorted(artists_ids))

    @staticmethod
    def _given_valid_audio_features_responses(
        spotify_test_client: SpotifyTestClient,
        playlists_resources_map: Dict[str, SpotifyPlaylistsResources],
    ) -> None:
        tracks_ids = chain_lists(
            [resource.track_ids for resource in playlists_resources_map.values()]
        )
        spotify_test_client.tracks.audio_features.expect_success(sorted(tracks_ids))

    async def _are_expected_db_records_inserted(
        self,
        db_engine: AsyncEngine,
        spotify_insertions_verifier: SpotifyInsertionsVerifier,
        playlist_resources_map: Dict[str, SpotifyPlaylistsResources],
    ):
        resources = list(playlist_resources_map.values())
        return await spotify_insertions_verifier.verify_playlist_resources(resources)
