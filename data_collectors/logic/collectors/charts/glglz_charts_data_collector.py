from textwrap import dedent
from typing import List, Optional

from genie_common.tools import logger, AioPoolExecutor
from genie_common.utils import chain_lists
from genie_datastores.postgres.models import ChartEntry
from google.generativeai import GenerativeModel
from playwright.async_api import Browser

from data_collectors.contract import IChartsDataCollector
from data_collectors.logic.models.daily_chart import DailyChart
from data_collectors.utils.gemini import serialize_generative_model_response
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
        prompt = """\
            In this task you are requested to extract from a raw HTML charts entries of two musical charts and the date
            of the chart. Each HTML contains total of 20 entries: 10 for each chart. One chart contains israeli songs 
            exclusively, and the other contains non-israeli songs exclusively. Each chart entries are grouped in a list,
            usually listed in ascending order, where the number prefix indicating the songs position. Each entry 
            contains both the name of the artist and the name of the track, usually separated by a colon.
            
            For example, a typical page would look like this after the HTML is rendered:
            
            ```
            17:47 | 03.09.2017

            המצעד הישראלי:
            1.שיר לוי - להשתגע
            2.דודו טסה - לשים ת'ראש
            3.Back to Black- עם אסף אבידן Red Band
            4.אברהם טל - את במרחבים
            5.שאנן סטריט - July
            6.אריק איינשטיין - אדם בחדרו
            7.אסתר רדא - Nanu Ney
            8.מאור אדרי - לא כואב לה
            9.אביתר בנאי - גנב
            10.בית הבובות - איפה היית
            
            המצעד הבינלאומי:
            Avicii -Addicted To You.1
            Katy Perry feat. Juicy J - Dark Horse.2
            Coldplay - Magic.3
            Clean Bandit feat. Jesse Glynne - Rather Be.4
            5.Lorde - Buzzcut Season
            Pharrell Williams - Happy.6
            Indila - Derniere Danse.7
            Sam Smith - Money On My Mind.8
            9.Beck - Blue Moon
            The Neighbourhood - Sweater Weather.10
            ```
             
            Please return JSON describing the date of the chart, and a list of chart entries using the following schema:

            {
                "date": datetime,
                "entries": List[RawEntry],
            }

            RawEntry = {raw_value: str, "artist": EntryArtist, "track": EntryTrack, "position": int, origin: ChartOrigin}
            ChartOrigin = Enum of the following values: ["israeli", "international"]
            EntryArtist = {"name": str, "translation": Optional[str]}
            EntryTrack = {"name": str, "translation": Optional[str]} 

            datetime fields should use the following format: `%Y-%m-%dT%H:%M:%s`.
            Position fields should return either an integer between 1-10.
            In models that ask for translation (such as EntryArtist and EntryTrack) you should translate only hebrew to 
            english, not vice versa. Your translation should not be a semantic translation, but a literal. For example,
            "פרח" should not be translated to "flower", but to "Perach".

            Important: Only return a single piece of valid JSON text.

            Here is the HTML:
        """

        response = await self._generative_model.generate_content_async(
            contents=dedent(prompt) + html,
            generation_config={"response_mime_type": "application/json"},
        )
        serialized_response: Optional[DailyChart] = serialize_generative_model_response(
            response=response, model=DailyChart
        )

        if serialized_response is None:
            return []

        return serialized_response.to_charts_entries(url)
