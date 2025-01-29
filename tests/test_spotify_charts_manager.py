from datetime import datetime, timedelta
from functools import partial
from http import HTTPStatus
from typing import Dict

from _pytest.fixtures import fixture
from genie_common.utils import chain_lists
from genie_datastores.postgres.models import ChartEntry
from genie_datastores.postgres.operations import execute_query
from spotipyio.testing import SpotifyTestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.components import ComponentFactory
from data_collectors.components.managers.charts_manager_factory import (
    SPOTIFY_PLAYLIST_CHART_MAP,
)
from data_collectors.jobs.job_builders.spotify_charts_job_builder import (
    SpotifyChartsJobBuilder,
)
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ScheduledJob
from main import lifespan
from tests.helpers.spotify_playlists_resources import SpotifyPlaylistsResources
from tests.testing_utils import until, app_test_client_session
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
        self._given_expected_spotify_responses(
            playlist_resources_map, spotify_test_client
        )

        with test_client as client:
            actual = client.post(f"jobs/trigger/{JobId.SPOTIFY_CHARTS.value}")

        assert actual.status_code == HTTPStatus.OK
        assert await self._are_expected_db_records_inserted(
            db_engine, spotify_insertions_verifier, playlist_resources_map
        )

    async def test_scheduled_job(
        self,
        spotify_test_client: SpotifyTestClient,
        scheduled_test_client: TestClient,
        playlist_resources_map: Dict[str, SpotifyPlaylistsResources],
        spotify_insertions_verifier: SpotifyInsertionsVerifier,
        db_engine: AsyncEngine,
    ):
        self._given_expected_spotify_responses(
            playlist_resources_map, spotify_test_client
        )
        condition = partial(
            self._are_expected_db_records_inserted,
            db_engine=db_engine,
            spotify_insertions_verifier=spotify_insertions_verifier,
            playlist_resources_map=playlist_resources_map,
        )

        with scheduled_test_client:
            await until(condition)

    @fixture
    async def scheduled_test_client(
        self,
        component_factory: ComponentFactory,
        spotify_charts_job: ScheduledJob,
    ) -> TestClient:
        lifespan_context = partial(
            lifespan,
            component_factory=component_factory,
            jobs={spotify_charts_job.id: spotify_charts_job},
        )

        with app_test_client_session(lifespan_context) as client:
            yield client

    @fixture
    async def spotify_charts_job(
        self, component_factory: ComponentFactory
    ) -> ScheduledJob:
        builder = SpotifyChartsJobBuilder(component_factory)
        next_run_time = datetime.now() + timedelta(seconds=2)

        return await builder.build(next_run_time=next_run_time)

    @fixture
    def playlist_resources_map(self) -> Dict[str, SpotifyPlaylistsResources]:
        playlists = list(SPOTIFY_PLAYLIST_CHART_MAP.keys())
        return PlaylistsResourcesCreator.create(playlists)

    def _given_expected_spotify_responses(
        self,
        playlist_resources_map: Dict[str, SpotifyPlaylistsResources],
        spotify_test_client: SpotifyTestClient,
    ):
        self._given_valid_playlists_responses(
            playlist_resources_map, spotify_test_client
        )
        self._given_valid_tracks_responses(playlist_resources_map, spotify_test_client)
        self._given_valid_artists_responses(spotify_test_client, playlist_resources_map)
        self._given_valid_audio_features_responses(
            spotify_test_client, playlist_resources_map
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
        are_spotify_records_inserted = (
            await spotify_insertions_verifier.verify_playlist_resources(resources)
        )

        if are_spotify_records_inserted:
            return await self._are_chart_entries_records_inserted(
                playlist_resources_map, db_engine
            )

        return False

    @staticmethod
    async def _are_chart_entries_records_inserted(
        playlist_resources_map: Dict[str, SpotifyPlaylistsResources],
        db_engine: AsyncEngine,
    ):
        for playlist_id, resources in playlist_resources_map.items():
            chart = SPOTIFY_PLAYLIST_CHART_MAP[playlist_id]
            query = select(ChartEntry.track_id).where(ChartEntry.chart == chart)
            query_result = await execute_query(engine=db_engine, query=query)
            actual = query_result.scalars().all()

            if sorted(actual) != sorted(resources.track_ids):
                return False

        return True
