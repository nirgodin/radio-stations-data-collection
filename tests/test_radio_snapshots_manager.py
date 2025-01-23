from asyncio import AbstractEventLoop
from datetime import datetime, timedelta
from functools import partial
from http import HTTPStatus
from typing import Dict, List

from genie_common.utils import chain_lists
from genie_datastores.postgres.models import SpotifyStation, RadioTrack
from genie_datastores.postgres.operations import execute_query
from joblib.testing import fixture
from spotipyio.testing import SpotifyTestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.components import ComponentFactory
from data_collectors.jobs.job_builders.radio_snapshots_job_builder import (
    RADIO_SNAPSHOTS_STATIONS,
    RadioSnapshotsJobBuilder,
)
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ScheduledJob
from main import lifespan
from tests.conftest import db_engine
from tests.helpers.spotify_playlists_resources import SpotifyPlaylistsResources
from tests.testing_utils import until, app_test_client_session
from tests.tools.playlists_resources_creator import PlaylistsResourcesCreator
from tests.tools.spotify_insertions_verifier import SpotifyInsertionsVerifier


class TestRadioSnapshotsManager:
    async def test_trigger(
        self,
        spotify_test_client: SpotifyTestClient,
        station_playlist_map: Dict[str, SpotifyPlaylistsResources],
        test_client: TestClient,
        spotify_insertions_verifier: SpotifyInsertionsVerifier,
        db_engine: AsyncEngine,
    ):
        self._given_expected_spotify_responses(
            spotify_test_client=spotify_test_client,
            station_playlist_map=station_playlist_map,
        )

        with test_client as client:
            actual = client.post(f"/jobs/trigger/{JobId.RADIO_SNAPSHOTS.value}")

        assert actual.status_code == HTTPStatus.OK
        assert await self._are_expected_db_records_inserted(
            spotify_insertions_verifier=spotify_insertions_verifier,
            station_playlist_map=station_playlist_map,
            db_engine=db_engine,
        )

    async def test_scheduled_job(
        self,
        spotify_test_client: SpotifyTestClient,
        station_playlist_map: Dict[str, SpotifyPlaylistsResources],
        scheduled_test_client: TestClient,
        spotify_insertions_verifier: SpotifyInsertionsVerifier,
        db_engine: AsyncEngine,
    ):
        self._given_expected_spotify_responses(
            spotify_test_client=spotify_test_client,
            station_playlist_map=station_playlist_map,
        )
        condition = partial(
            self._are_expected_db_records_inserted,
            spotify_insertions_verifier=spotify_insertions_verifier,
            station_playlist_map=station_playlist_map,
            db_engine=db_engine,
        )

        with scheduled_test_client:
            await until(condition)

    @fixture
    async def scheduled_test_client(
        self,
        component_factory: ComponentFactory,
        radio_snapshots_job: ScheduledJob,
        event_loop: AbstractEventLoop,
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
        next_run_time = datetime.now() + timedelta(seconds=2)

        return await builder.build(next_run_time=next_run_time)

    @fixture
    def station_playlist_map(self) -> Dict[str, SpotifyPlaylistsResources]:
        ids = [station.value for station in RADIO_SNAPSHOTS_STATIONS]
        return PlaylistsResourcesCreator.create(ids)

    def _given_expected_spotify_responses(
        self,
        spotify_test_client: SpotifyTestClient,
        station_playlist_map: Dict[str, SpotifyPlaylistsResources],
    ) -> None:
        self._given_valid_playlists_response(spotify_test_client, station_playlist_map)
        self._given_valid_artists_responses(spotify_test_client, station_playlist_map)
        self._given_valid_audio_features_responses(
            spotify_test_client, station_playlist_map
        )

    @staticmethod
    def _given_valid_playlists_response(
        spotify_test_client: SpotifyTestClient,
        station_playlist_map: Dict[str, SpotifyPlaylistsResources],
    ) -> None:
        for playlist_id, playlist_resources in station_playlist_map.items():
            spotify_test_client.playlists.info.expect_success(
                playlist_id, [playlist_resources.playlist]
            )

    @staticmethod
    def _given_valid_artists_responses(
        spotify_test_client: SpotifyTestClient,
        station_playlist_map: Dict[str, SpotifyPlaylistsResources],
    ) -> None:
        for playlist_resources in station_playlist_map.values():
            sorted_artists = sorted(playlist_resources.artists)
            # The call to artists endpoint is made twice, by the ArtistsInserter and by the RadioTracksInserter
            spotify_test_client.artists.info.expect_success(sorted_artists)
            spotify_test_client.artists.info.expect_success(sorted_artists)

    @staticmethod
    def _given_valid_audio_features_responses(
        spotify_test_client: SpotifyTestClient,
        station_playlist_map: Dict[str, SpotifyPlaylistsResources],
    ) -> None:
        for playlist_resources in station_playlist_map.values():
            spotify_test_client.tracks.audio_features.expect_success(
                sorted(playlist_resources.tracks),
            )

    async def _are_expected_db_records_inserted(
        self,
        spotify_insertions_verifier: SpotifyInsertionsVerifier,
        station_playlist_map: Dict[str, SpotifyPlaylistsResources],
        db_engine: AsyncEngine,
    ) -> bool:
        are_spotify_records_inserted = (
            await self._are_expected_spotify_records_inserted(
                spotify_insertions_verifier, station_playlist_map
            )
        )

        if are_spotify_records_inserted:
            return await self._are_expected_radio_tracks_records_inserted(
                station_playlist_map, db_engine
            )

        return False

    @staticmethod
    async def _are_expected_spotify_records_inserted(
        spotify_insertions_verifier: SpotifyInsertionsVerifier,
        station_playlist_map: Dict[str, SpotifyPlaylistsResources],
    ) -> bool:
        tracks = chain_lists(
            [resources.tracks for resources in station_playlist_map.values()]
        )
        artists = chain_lists(
            [resources.artists for resources in station_playlist_map.values()]
        )
        albums = chain_lists(
            [resources.albums for resources in station_playlist_map.values()]
        )

        return await spotify_insertions_verifier.verify(
            artists=artists,
            tracks=tracks,
            albums=albums,
        )

    @staticmethod
    async def _are_expected_radio_tracks_records_inserted(
        station_playlist_map: Dict[str, SpotifyPlaylistsResources],
        db_engine: AsyncEngine,
    ) -> bool:
        for playlist_id, playlist_resources in station_playlist_map.items():
            query = select(RadioTrack.track_id).where(
                RadioTrack.station == SpotifyStation(playlist_id)
            )
            query_result = await execute_query(db_engine, query)
            actual = query_result.scalars().all()

            if not sorted(actual) == sorted(playlist_resources.tracks):
                return False

        return True
