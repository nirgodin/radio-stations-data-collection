from datetime import datetime
from http import HTTPStatus
from random import randint, shuffle, choice
from typing import Optional, Dict
from urllib.parse import unquote

from _pytest.fixtures import fixture
from genie_common.utils import random_datetime
from spotipyio.logic.utils import random_alphanumeric_string
from sqlalchemy.ext.asyncio import AsyncEngine
from starlette.testclient import TestClient

from data_collectors.consts.glglz_consts import WEEKLY_CHART_PREFIX, GLGLZ_CHARTS_ARCHIVE_ROUTE
from data_collectors.jobs.job_id import JobId
from tests.tools.playwright_testkit import PlaywrightTestkit


class TestGlglzChartsManager:
    async def test_trigger(
        self,
        db_engine: AsyncEngine,
        test_client: TestClient,
        charts_archive_page: str,
        charts_dates_to_pages: Dict[datetime, str],
        playwright_testkit: PlaywrightTestkit,
    ):
        self._expect_charts_archive_request(playwright_testkit, charts_archive_page)
        self._expect_charts_pages_requests(playwright_testkit, charts_dates_to_pages)

        with test_client as client:
            actual = client.post(f"jobs/trigger/{JobId.GLGLZ_CHARTS.value}")

        assert actual.status_code == HTTPStatus.OK

    @staticmethod
    def _expect_charts_archive_request(playwright_testkit: PlaywrightTestkit, charts_archive_page: str) -> None:
        playwright_testkit.expect(uri=f"/{unquote(GLGLZ_CHARTS_ARCHIVE_ROUTE)}", html=charts_archive_page)

    def _expect_charts_pages_requests(
        self, playwright_testkit: PlaywrightTestkit, charts_dates_to_pages: Dict[datetime, str]
    ) -> None:
        for date, page in charts_dates_to_pages.items():
            route = self._to_glglz_route(date)
            playwright_testkit.expect(uri=f"/{route}", html=page)

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
    def charts_archive_page(self, charts_dates_to_pages: Dict[datetime, str]) -> str:
        glglz_links = [self._a_glglz_html_link(date) for date in charts_dates_to_pages]
        non_glglz_links = [self._an_html_link() for _ in range(randint(0, 10))]
        links = glglz_links + non_glglz_links
        shuffle(links)
        formatted_links = "".join(links)

        return f'<html><head><meta charset="UTF-8"></head><body>{formatted_links}</body></html>'

    @fixture
    def charts_dates_to_pages(self) -> Dict[datetime, str]:
        return {
            random_datetime(): random_alphanumeric_string(min_length=10),
            random_datetime(): random_alphanumeric_string(min_length=10),
        }
