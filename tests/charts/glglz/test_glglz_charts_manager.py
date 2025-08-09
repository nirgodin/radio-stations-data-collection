from datetime import datetime
from functools import partial
from http import HTTPStatus
from random import randint, shuffle, choice
from typing import Optional, Dict, List
from unittest.mock import AsyncMock
from urllib.parse import unquote

from _pytest.fixtures import fixture
from genie_common.utils import chain_lists
from genie_datastores.postgres.models import ChartEntry, Chart
from genie_datastores.postgres.operations import execute_query
from spotipyio.logic.utils import random_alphanumeric_string
from spotipyio.models import SearchItem, SearchItemMetadata, SpotifySearchType
from spotipyio.testing import SpotifyTestClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.components import ComponentFactory
from data_collectors.consts.glglz_consts import WEEKLY_CHART_PREFIX, GLGLZ_CHARTS_ARCHIVE_ROUTE
from data_collectors.jobs.job_builders.charts.glglz_charts_job_builder import GlglzChartsJobBuilder
from data_collectors.jobs.job_id import JobId
from tests.charts.glglz.glglz_chart_resources import GlglzChartResources
from tests.helpers.mock_generate_content_response import MockGenerateContentResponse
from tests.testing_utils import build_scheduled_test_client, until
from tests.tools.playwright_testkit import PlaywrightTestkit
from tests.tools.spotify_insertions_verifier import SpotifyInsertionsVerifier


class TestGlglzChartsManager:
    async def test_trigger(
        self,
        db_engine: AsyncEngine,
        test_client: TestClient,
        charts_archive_page: str,
        charts_resources: List[GlglzChartResources],
        playwright_testkit: PlaywrightTestkit,
        mock_gemini_model: AsyncMock,
        spotify_test_client: SpotifyTestClient,
        spotify_insertions_verifier: SpotifyInsertionsVerifier,
    ):
        self._given_expected_requests(
            playwright_testkit=playwright_testkit,
            charts_archive_page=charts_archive_page,
            charts_resources=charts_resources,
            mock_gemini_model=mock_gemini_model,
            spotify_test_client=spotify_test_client,
        )

        with test_client as client:
            actual = client.post(f"jobs/trigger/{JobId.GLGLZ_CHARTS.value}")

        assert actual.status_code == HTTPStatus.OK
        assert await self._are_expected_db_records_inserted(
            charts_resources=charts_resources,
            spotify_insertions_verifier=spotify_insertions_verifier,
            db_engine=db_engine,
        )

    async def test_scheduled(
        self,
        db_engine: AsyncEngine,
        charts_archive_page: str,
        charts_resources: List[GlglzChartResources],
        playwright_testkit: PlaywrightTestkit,
        mock_gemini_model: AsyncMock,
        spotify_test_client: SpotifyTestClient,
        spotify_insertions_verifier: SpotifyInsertionsVerifier,
        scheduled_test_client: TestClient,
    ):
        self._given_expected_requests(
            playwright_testkit=playwright_testkit,
            charts_archive_page=charts_archive_page,
            charts_resources=charts_resources,
            mock_gemini_model=mock_gemini_model,
            spotify_test_client=spotify_test_client,
        )
        condition = partial(
            self._are_expected_db_records_inserted,
            charts_resources,
            spotify_insertions_verifier,
            db_engine,
        )

        with scheduled_test_client:
            await until(condition)

    def _given_expected_requests(
        self,
        playwright_testkit: PlaywrightTestkit,
        charts_archive_page: str,
        charts_resources: List[GlglzChartResources],
        mock_gemini_model: AsyncMock,
        spotify_test_client: SpotifyTestClient,
    ) -> None:
        self._expect_charts_archive_request(playwright_testkit, charts_archive_page)
        self._expect_charts_pages_requests(playwright_testkit, charts_resources)
        self._given_valid_gemini_responses(mock_gemini_model, charts_resources)
        self._given_valid_search_responses(spotify_test_client, charts_resources)
        self._given_valid_artists_responses(spotify_test_client, charts_resources)
        self._given_valid_audio_features_responses(spotify_test_client, charts_resources)

    @staticmethod
    def _expect_charts_archive_request(playwright_testkit: PlaywrightTestkit, charts_archive_page: str) -> None:
        playwright_testkit.expect(uri=f"/{unquote(GLGLZ_CHARTS_ARCHIVE_ROUTE)}", html=charts_archive_page)

    def _expect_charts_pages_requests(
        self,
        playwright_testkit: PlaywrightTestkit,
        charts_resources: List[GlglzChartResources],
    ) -> None:
        for resource in charts_resources:
            route = self._to_glglz_route(resource.date)
            playwright_testkit.expect(uri=f"/{route}", html=resource.html)

    def _a_glglz_html_link(self, date: datetime) -> str:
        text = self._to_glglz_route(date)
        return self._an_html_link(href=text, text=text)

    @staticmethod
    def _to_glglz_route(date: datetime) -> str:
        formatted_date = date.strftime("%d%m%Y")
        return f"{WEEKLY_CHART_PREFIX}-{formatted_date}"

    def _an_html_link(self, href: Optional[str] = None, text: Optional[str] = None) -> str:
        if text is None:
            text = random_alphanumeric_string()

        if href is None:
            href = self._random_url()

        return f'<a href="{href}">{text}</a>'

    @staticmethod
    def _random_url() -> str:
        protocol = choice(["http", "https"])
        subdomain = choice(["www", ""])
        domain = random_alphanumeric_string()
        tld = choice(["com", "net", "org", "io", "co"])

        return f"{protocol}://{subdomain}.{domain}.{tld}"

    @staticmethod
    def _given_valid_gemini_responses(
        mock_gemini_model: AsyncMock,
        charts_resources: List[GlglzChartResources],
    ) -> None:
        def _mock_generate_content(contents: str, generation_config: Dict[str, str]) -> MockGenerateContentResponse:
            for resource in charts_resources:
                if contents.__contains__(resource.html):
                    text = resource.chart_details.json()
                    return MockGenerateContentResponse(
                        parts=[text],
                        text=text,
                    )

        mock_gemini_model.side_effect = _mock_generate_content

    @staticmethod
    def _given_valid_search_responses(
        spotify_test_client: SpotifyTestClient, charts_resources: List[GlglzChartResources]
    ) -> None:
        for resource in charts_resources:
            for entry_resources in resource.entries_resources:
                search_item = SearchItem(
                    text=entry_resources.to_search_query(),
                    metadata=SearchItemMetadata(search_types=[SpotifySearchType.TRACK], quote=False),
                )
                spotify_test_client.search.search_item.expect_success(
                    search_item=search_item,
                    response_json=entry_resources.to_search_response(),
                )

    @staticmethod
    def _given_valid_artists_responses(
        spotify_test_client: SpotifyTestClient, charts_resources: List[GlglzChartResources]
    ) -> None:
        artists_ids = chain_lists([resource.get_artists_ids() for resource in charts_resources])
        spotify_test_client.artists.info.expect_success(sorted(artists_ids))

    @staticmethod
    def _given_valid_audio_features_responses(
        spotify_test_client: SpotifyTestClient, charts_resources: List[GlglzChartResources]
    ) -> None:
        tracks_ids = chain_lists([resource.get_tracks_ids() for resource in charts_resources])
        spotify_test_client.tracks.audio_features.expect_success(sorted(tracks_ids))

    async def _are_expected_db_records_inserted(
        self,
        charts_resources: List[GlglzChartResources],
        spotify_insertions_verifier: SpotifyInsertionsVerifier,
        db_engine: AsyncEngine,
    ) -> bool:
        are_spotify_records_inserted = await self._are_expected_spotify_entries_inserted(
            charts_resources, spotify_insertions_verifier
        )

        if are_spotify_records_inserted:
            return await self._are_chart_entries_records_inserted(charts_resources, db_engine)

        return False

    @staticmethod
    async def _are_expected_spotify_entries_inserted(
        charts_resources: List[GlglzChartResources], spotify_insertions_verifier: SpotifyInsertionsVerifier
    ) -> bool:
        tracks_ids = chain_lists([resource.get_tracks_ids() for resource in charts_resources])
        artists_ids = chain_lists([resource.get_artists_ids() for resource in charts_resources])
        albums_ids = chain_lists([resource.get_albums_ids() for resource in charts_resources])

        return await spotify_insertions_verifier.verify(
            artists=artists_ids,
            tracks=tracks_ids,
            albums=albums_ids,
        )

    @staticmethod
    async def _are_chart_entries_records_inserted(
        charts_resources: List[GlglzChartResources], db_engine: AsyncEngine
    ) -> bool:
        expected = chain_lists([resource.get_tracks_ids() for resource in charts_resources])
        query = select(ChartEntry.track_id).where(
            ChartEntry.chart.in_([Chart.GLGLZ_WEEKLY_ISRAELI, Chart.GLGLZ_WEEKLY_INTERNATIONAL])
        )
        query_result = await execute_query(engine=db_engine, query=query)
        actual = query_result.scalars().all()

        return sorted(actual) == sorted(expected)

    @fixture
    def charts_archive_page(self, charts_resources: List[GlglzChartResources]) -> str:
        glglz_links = [self._a_glglz_html_link(resource.date) for resource in charts_resources]
        non_glglz_links = [self._an_html_link() for _ in range(randint(0, 10))]
        links = glglz_links + non_glglz_links
        shuffle(links)
        formatted_links = "".join(links)

        return f'<html><head><meta charset="UTF-8"></head><body>{formatted_links}</body></html>'

    @fixture
    def charts_resources(self) -> List[GlglzChartResources]:
        return [GlglzChartResources.random()]

    @fixture
    async def scheduled_test_client(
        self,
        component_factory: ComponentFactory,
    ) -> TestClient:
        scheduled_client = await build_scheduled_test_client(component_factory, GlglzChartsJobBuilder)

        with scheduled_client as client:
            yield client
