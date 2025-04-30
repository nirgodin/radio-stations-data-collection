from textwrap import dedent
from typing import List, Optional

from genie_common.tools import logger, AioPoolExecutor
from genie_common.utils import chain_lists
from genie_datastores.postgres.models import ChartEntry
from google.generativeai import GenerativeModel
from playwright.async_api import Browser

from data_collectors.contract import IChartsDataCollector
from data_collectors.logic.models.daily_chart import DailyChart
from data_collectors.utils.gemini import serialize_generative_model_response, load_prompt
from data_collectors.utils.playwright import get_page_content


class GlglzChartsDataCollector(IChartsDataCollector):
    def __init__(
        self,
        pool_executor: AioPoolExecutor,
        browser: Browser,
        generative_model: GenerativeModel,
    ):
        self._pool_executor = pool_executor
        self._browser = browser
        self._generative_model = generative_model

    async def collect(self, urls: List[str]) -> List[ChartEntry]:
        logger.info("Starting to run GlglzChartsDataCollector to collect raw charts entries")
        if not urls:
            logger.info("GlglzChartsDataCollector did not receive any URL to crawl. Returning empty list")
            return []

        charts_entries = []
        entries = await self._pool_executor.run(
            iterable=urls,
            func=self._collect_single_date_details,
            expected_type=list,
        )
        charts_entries.extend(chain_lists(entries))

        return charts_entries

    async def _collect_single_date_details(self, url: str) -> Optional[List[ChartEntry]]:
        logger.info(f"Fetching raw chart HTML from `{url}`")
        page_source = await self._fetch_page_source(url)

        if self._is_ok_response(page_source, url):
            return await self._extract_charts_entries_from_html(page_source, url)

        logger.warn(f"Did not find chart page for url `{url}`. Skipping")

    async def _fetch_page_source(self, url: str) -> Optional[str]:
        page = await self._browser.new_page()
        await page.goto(url)

        return await get_page_content(page)

    @staticmethod
    def _is_ok_response(page_source: str, url: str) -> bool:
        if "custom 404" in page_source.lower():
            logger.info(f"Did not manage to find charts entries in url `{url}`. Skipping")
            return False

        logger.info(f"Found charts entries in url `{url}`! Parsing page source")
        return True

    async def _extract_charts_entries_from_html(self, html: str, url: str) -> List[ChartEntry]:
        prompt = load_prompt("glglz_charts_prompt.txt")
        response = await self._generative_model.generate_content_async(
            contents=dedent(prompt) + f"\n```\n{html}\n```",
            generation_config={"response_mime_type": "application/json"},
        )
        serialized_response: Optional[DailyChart] = serialize_generative_model_response(
            response=response, model=DailyChart
        )

        if serialized_response is None:
            return []

        return serialized_response.to_charts_entries(url)
