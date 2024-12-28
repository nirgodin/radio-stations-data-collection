from http import HTTPStatus
from typing import Dict

from genie_datastores.postgres.models import SpotifyStation
from joblib.testing import fixture
from spotipyio.testing import SpotifyTestClient, SpotifyMockFactory
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.jobs.radio_snapshots_job_builder import RADIO_SNAPSHOTS_STATIONS
from data_collectors.utils.spotify import extract_unique_artists_ids


class TestRadioSnapshotsManager:
    async def test_trigger(self,
                           spotify_test_client: SpotifyTestClient,
                           station_playlist_map: Dict[SpotifyStation, dict],
                           test_client: TestClient,
                           db_engine: AsyncEngine):
        self._given_valid_playlists_response(spotify_test_client, station_playlist_map)
        self._given_valid_artists_responses(spotify_test_client, station_playlist_map)
        self._given_valid_audio_features_responses(spotify_test_client, station_playlist_map)

        actual = test_client.post("/jobs/trigger/radio_snapshots")

        assert actual.status_code == HTTPStatus.OK

    @staticmethod
    def _given_valid_playlists_response(spotify_test_client: SpotifyTestClient,
                                        station_playlist_map: Dict[SpotifyStation, dict]) -> None:
        for station, playlist in station_playlist_map.items():
            spotify_test_client.playlists.info.expect_success(station.value, [playlist])

    @staticmethod
    def _given_valid_artists_responses(spotify_test_client: SpotifyTestClient,
                                       station_playlist_map: Dict[SpotifyStation, dict]) -> None:
        for station, playlist in station_playlist_map.items():
            playlist_tracks = playlist["tracks"]["items"]
            playlist_artists = extract_unique_artists_ids(*playlist_tracks)

            spotify_test_client.artists.info.expect_success(sorted(playlist_artists))
            spotify_test_client.artists.info.expect_success(sorted(playlist_artists))

    @staticmethod
    def _given_valid_audio_features_responses(spotify_test_client: SpotifyTestClient,
                                              station_playlist_map: Dict[SpotifyStation, dict]) -> None:
        for station, playlist in station_playlist_map.items():
            playlist_tracks = playlist["tracks"]["items"]
            playlist_tracks_ids = [track["track"]["id"] for track in playlist_tracks]

            spotify_test_client.tracks.audio_features.expect_success(sorted(playlist_tracks_ids))

    @fixture
    def station_playlist_map(self) -> Dict[SpotifyStation, dict]:
        return {station: SpotifyMockFactory.playlist(id=station.value) for station in RADIO_SNAPSHOTS_STATIONS}
