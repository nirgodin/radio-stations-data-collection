import json
from datetime import datetime
from http import HTTPStatus
from random import randint, shuffle, choice
from typing import Optional, Dict, List
from unittest.mock import AsyncMock
from urllib.parse import unquote

from _pytest.fixtures import fixture
from genie_common.utils import random_datetime
from spotipyio.logic.utils import random_alphanumeric_string
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.consts.glglz_consts import WEEKLY_CHART_PREFIX, GLGLZ_CHARTS_ARCHIVE_ROUTE
from data_collectors.jobs.job_id import JobId
from tests.charts.glglz.glglz_chart_resources import GlglzChartResources
from tests.helpers.mock_generate_content_response import MockGenerateContentResponse
from tests.tools.playwright_testkit import PlaywrightTestkit


class TestGlglzChartsManager:
    async def test_trigger(
        self,
        db_engine: AsyncEngine,
        test_client: TestClient,
        charts_archive_page: str,
        charts_resources: List[GlglzChartResources],
        playwright_testkit: PlaywrightTestkit,
        mock_gemini_model: AsyncMock,
    ):
        self._expect_charts_archive_request(playwright_testkit, charts_archive_page)
        self._expect_charts_pages_requests(playwright_testkit, charts_resources)
        self._given_valid_gemini_responses(mock_gemini_model, charts_resources)

        with test_client as client:
            actual = client.post(f"jobs/trigger/{JobId.GLGLZ_CHARTS.value}")

        assert actual.status_code == HTTPStatus.OK

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
        return [GlglzChartResources.random(), GlglzChartResources.random()]

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
