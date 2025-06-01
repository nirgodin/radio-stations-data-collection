from datetime import datetime
from functools import partial
from http import HTTPStatus
from random import randint
from typing import List

import pandas as pd
from genie_datastores.postgres.models import ChartEntry, Chart
from genie_datastores.postgres.operations import insert_records, execute_query
from joblib.testing import fixture
from pandas import DataFrame
from spotipyio.models import SearchItem, SearchItemMetadata, SpotifySearchType
from spotipyio.testing import SpotifyTestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.components import ComponentFactory
from data_collectors.consts.eurovision_consts import (
    EUROVISION_WIKIPEDIA_PAGE_TITLE_FORMAT,
    EUROVISION_ARTIST_COLUMN,
    EUROVISION_SONG_COLUMN,
    EUROVISION_POINTS_COLUMN,
    EUROVISION_PLACE_COLUMN,
)
from data_collectors.consts.spotify_consts import TRACKS, ITEMS
from data_collectors.jobs.job_builders.charts.eurovision_chart_job_builder import (
    EurovisionChartJobBuilder,
)
from data_collectors.jobs.job_id import JobId
from tests.helpers.spotify_track_resources import SpotifyTrackResources
from tests.testing_utils import until, build_scheduled_test_client
from tests.tools.spotify_insertions_verifier import SpotifyInsertionsVerifier
from tests.tools.wikipedia_test_client import WikipediaTestClient


class TestEurovisionChartsManager:
    async def test_trigger(
        self,
        db_engine: AsyncEngine,
        year: int,
        wikipedia_test_client: WikipediaTestClient,
        wikipedia_summary_table: DataFrame,
        spotify_test_client: SpotifyTestClient,
        tracks_resources: List[SpotifyTrackResources],
        test_client: TestClient,
        spotify_insertions_verifier: SpotifyInsertionsVerifier,
    ):
        await self._given_existing_eurovision_chart_entry(db_engine, year)
        self._given_valid_wikipedia_response(wikipedia_test_client, year, wikipedia_summary_table)
        self._given_valid_spotify_responses(spotify_test_client, tracks_resources)

        with test_client as client:
            actual = client.post(f"jobs/trigger/{JobId.EUROVISION_CHARTS.value}")

        assert actual.status_code == HTTPStatus.OK
        assert await self._are_expected_db_records_inserted(
            tracks_resources=tracks_resources,
            spotify_insertions_verifier=spotify_insertions_verifier,
            db_engine=db_engine,
            year=year,
        )

    async def test_scheduled_job(
        self,
        scheduled_test_client: TestClient,
        db_engine: AsyncEngine,
        year: int,
        wikipedia_test_client: WikipediaTestClient,
        wikipedia_summary_table: DataFrame,
        spotify_test_client: SpotifyTestClient,
        tracks_resources: List[SpotifyTrackResources],
        spotify_insertions_verifier: SpotifyInsertionsVerifier,
    ):
        await self._given_existing_eurovision_chart_entry(db_engine, year)
        self._given_valid_wikipedia_response(wikipedia_test_client, year, wikipedia_summary_table)
        self._given_valid_spotify_responses(spotify_test_client, tracks_resources)
        condition = partial(
            self._are_expected_db_records_inserted,
            tracks_resources=tracks_resources,
            spotify_insertions_verifier=spotify_insertions_verifier,
            db_engine=db_engine,
            year=year,
        )

        with scheduled_test_client:
            await until(condition)

    @fixture
    def year(self) -> int:
        return randint(1950, 2024)

    @fixture
    def wikipedia_summary_table(self, tracks_resources: List[SpotifyTrackResources]) -> DataFrame:
        records = []

        for i, resource in enumerate(tracks_resources):
            record = {
                EUROVISION_ARTIST_COLUMN: resource.artist_name,
                EUROVISION_SONG_COLUMN: resource.track_name,
                EUROVISION_POINTS_COLUMN: randint(1, 100),
                EUROVISION_PLACE_COLUMN: i + 1,
            }
            records.append(record)

        return pd.DataFrame.from_records(records)

    @fixture
    def tracks_resources(self) -> List[SpotifyTrackResources]:
        return [SpotifyTrackResources.random() for _ in range(randint(1, 10))]

    @fixture
    async def scheduled_test_client(
        self,
        component_factory: ComponentFactory,
    ) -> TestClient:
        scheduled_client = await build_scheduled_test_client(component_factory, EurovisionChartJobBuilder)
        with scheduled_client as client:
            yield client

    @staticmethod
    async def _given_existing_eurovision_chart_entry(db_engine: AsyncEngine, year: int) -> None:
        last_year = year - 1
        entry = ChartEntry(
            chart=Chart.EUROVISION,
            date=datetime(last_year, 1, 1),
            position=randint(1, 20),
        )

        await insert_records(engine=db_engine, records=[entry])

    @staticmethod
    def _given_valid_wikipedia_response(
        wikipedia_test_client: WikipediaTestClient,
        year: int,
        wikipedia_summary_table: DataFrame,
    ) -> None:
        title = EUROVISION_WIKIPEDIA_PAGE_TITLE_FORMAT.format(year=year)
        wikipedia_test_client.given_valid_response(title=title, response=wikipedia_summary_table.to_html())

    def _given_valid_spotify_responses(
        self,
        spotify_test_client: SpotifyTestClient,
        tracks_resources: List[SpotifyTrackResources],
    ) -> None:
        self._given_valid_spotify_search_responses(spotify_test_client, tracks_resources)
        tracks_ids = sorted([resource.track_id for resource in tracks_resources])
        artists_ids = sorted([resource.artist_id for resource in tracks_resources])
        spotify_test_client.artists.info.expect_success(sorted(artists_ids))
        spotify_test_client.tracks.audio_features.expect_success(sorted(tracks_ids))

    @staticmethod
    def _given_valid_spotify_search_responses(
        spotify_test_client: SpotifyTestClient,
        tracks_resources: List[SpotifyTrackResources],
    ) -> None:
        for resource in tracks_resources:
            key = f"{resource.artist_name} - {resource.track_name}"
            search_item = SearchItem(
                text=key,
                metadata=SearchItemMetadata(search_types=[SpotifySearchType.TRACK], quote=False),
            )
            response = {
                TRACKS: {
                    ITEMS: [
                        resource.track,
                    ]
                }
            }
            spotify_test_client.search.search_item.expect_success(search_item, response)

    async def _are_expected_db_records_inserted(
        self,
        tracks_resources: List[SpotifyTrackResources],
        spotify_insertions_verifier: SpotifyInsertionsVerifier,
        db_engine: AsyncEngine,
        year: int,
    ) -> bool:
        tracks_ids = [resource.track_id for resource in tracks_resources]
        are_expected_spotify_records_inserted = await spotify_insertions_verifier.verify(
            artists=[resource.artist_id for resource in tracks_resources],
            tracks=tracks_ids,
            albums=[resource.album_id for resource in tracks_resources],
        )

        if are_expected_spotify_records_inserted:
            return await self._are_expected_chart_entries_inserted(
                tracks_ids=tracks_ids, db_engine=db_engine, year=year
            )

        return False

    @staticmethod
    async def _are_expected_chart_entries_inserted(tracks_ids: List[str], db_engine: AsyncEngine, year: int) -> bool:
        query = (
            select(ChartEntry.track_id)
            .where(ChartEntry.date == datetime(year, 1, 1))
            .where(ChartEntry.chart == Chart.EUROVISION)
        )
        query_result = await execute_query(engine=db_engine, query=query)
        actual = query_result.scalars().all()

        return sorted(tracks_ids) == sorted(actual)
