from asyncio import AbstractEventLoop
from functools import partial
from http import HTTPStatus
from typing import Dict, List

from apscheduler.triggers.interval import IntervalTrigger
from genie_common.utils import chain_lists
from genie_datastores.postgres.models import SpotifyStation
from joblib.testing import fixture
from spotipyio.testing import SpotifyTestClient, SpotifyMockFactory
from starlette.testclient import TestClient

from data_collectors.components import ComponentFactory
from data_collectors.jobs.radio_snapshots_job_builder import (
    RADIO_SNAPSHOTS_STATIONS,
    RadioSnapshotsJobBuilder,
)
from data_collectors.logic.models import ScheduledJob
from data_collectors.utils.spotify import extract_unique_artists_ids
from main import lifespan
from tests.testing_utils import until, app_test_client_session
from tests.tools.spotify_insertions_verifier import SpotifyInsertionsVerifier


class TestRadioSnapshotsManager:
    async def test_trigger(
        self,
        spotify_test_client: SpotifyTestClient,
        station_playlist_map: Dict[SpotifyStation, dict],
        artists: List[List[str]],
        tracks: List[List[str]],
        albums: List[str],
        test_client: TestClient,
        spotify_insertions_verifier: SpotifyInsertionsVerifier,
    ):
        self._given_expected_spotify_responses(
            spotify_test_client=spotify_test_client,
            station_playlist_map=station_playlist_map,
            artists=artists,
            tracks=tracks,
        )

        actual = test_client.post("/jobs/trigger/radio_snapshots")

        assert actual.status_code == HTTPStatus.OK
        assert spotify_insertions_verifier.verify(
            artists=chain_lists(artists),
            tracks=chain_lists(tracks),
            albums=albums,
        )

    async def test_scheduled_job(
        self,
        spotify_test_client: SpotifyTestClient,
        station_playlist_map: Dict[SpotifyStation, dict],
        artists: List[List[str]],
        tracks: List[List[str]],
        albums: List[str],
        scheduled_test_client: TestClient,
        spotify_insertions_verifier: SpotifyInsertionsVerifier,
    ):
        self._given_expected_spotify_responses(
            spotify_test_client=spotify_test_client,
            station_playlist_map=station_playlist_map,
            artists=artists,
            tracks=tracks,
        )
        condition = partial(
            spotify_insertions_verifier.verify,
            artists=chain_lists(artists),
            tracks=chain_lists(tracks),
            albums=albums,
        )

        with scheduled_test_client:
            await until(condition)

    @fixture
    async def scheduled_test_client(
        self, component_factory: ComponentFactory, radio_snapshots_job: ScheduledJob, event_loop: AbstractEventLoop
    ) -> TestClient:
        lifespan_context = partial(
            lifespan,
            component_factory=component_factory,
            jobs={radio_snapshots_job.id: radio_snapshots_job},
        )

        with app_test_client_session(lifespan_context) as client:
            yield client

    @fixture
    async def radio_snapshots_job(
        self, component_factory: ComponentFactory
    ) -> ScheduledJob:
        builder = RadioSnapshotsJobBuilder(component_factory)
        interval = IntervalTrigger(seconds=1)

        return await builder.build(interval=interval)

    @fixture
    def station_playlist_map(self) -> Dict[SpotifyStation, dict]:
        return {
            station: SpotifyMockFactory.playlist(id=station.value)
            for station in RADIO_SNAPSHOTS_STATIONS
        }

    @fixture
    def artists(
        self, station_playlist_map: Dict[SpotifyStation, dict]
    ) -> List[List[str]]:
        artists_ids = []

        for playlist in station_playlist_map.values():
            playlist_tracks = playlist["tracks"]["items"]
            playlist_artists = extract_unique_artists_ids(*playlist_tracks)
            artists_ids.append(list(playlist_artists))

        return artists_ids

    @fixture
    def tracks(
        self, station_playlist_map: Dict[SpotifyStation, dict]
    ) -> List[List[str]]:
        tracks_ids = []

        for playlist in station_playlist_map.values():
            playlist_tracks = playlist["tracks"]["items"]
            playlist_tracks_ids = [track["track"]["id"] for track in playlist_tracks]
            tracks_ids.append(playlist_tracks_ids)

        return tracks_ids

    @fixture
    def albums(self, station_playlist_map: Dict[SpotifyStation, dict]) -> List[str]:
        albums_ids = []

        for playlist in station_playlist_map.values():
            playlist_tracks = playlist["tracks"]["items"]
            playlist_albums_ids = [
                track["track"]["album"]["id"] for track in playlist_tracks
            ]
            albums_ids.extend(playlist_albums_ids)

        return albums_ids

    def _given_expected_spotify_responses(
        self,
        spotify_test_client: SpotifyTestClient,
        station_playlist_map: Dict[SpotifyStation, dict],
        artists: List[List[str]],
        tracks: List[List[str]],
    ) -> None:
        self._given_valid_playlists_response(spotify_test_client, station_playlist_map)
        self._given_valid_artists_responses(spotify_test_client, artists)
        self._given_valid_audio_features_responses(spotify_test_client, tracks)

    @staticmethod
    def _given_valid_playlists_response(
        spotify_test_client: SpotifyTestClient,
        station_playlist_map: Dict[SpotifyStation, dict],
    ) -> None:
        for station, playlist in station_playlist_map.items():
            spotify_test_client.playlists.info.expect_success(station.value, [playlist])

    @staticmethod
    def _given_valid_artists_responses(
        spotify_test_client: SpotifyTestClient, artists: List[List[str]]
    ) -> None:
        for playlist_artists in artists:
            sorted_artists = sorted(playlist_artists)
            # The call to artists endpoint is made twice, by the ArtistsInserter and by the RadioTracksInserter
            spotify_test_client.artists.info.expect_success(sorted_artists)
            spotify_test_client.artists.info.expect_success(sorted_artists)

    @staticmethod
    def _given_valid_audio_features_responses(
        spotify_test_client: SpotifyTestClient, tracks: List[List[str]]
    ) -> None:
        for station_tracks in tracks:
            spotify_test_client.tracks.audio_features.expect_success(
                sorted(station_tracks)
            )
