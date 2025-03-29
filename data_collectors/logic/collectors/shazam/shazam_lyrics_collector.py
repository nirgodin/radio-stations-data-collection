from functools import partial
from typing import Optional, Tuple, List, Dict

from genie_common.tools import AioPoolExecutor, logger
from playwright.async_api import async_playwright, Browser

from data_collectors.consts.shazam_consts import (
    SHAZAM_TRACK_URL_FORMAT,
    SHAZAM_LYRICS_WEB_ELEMENT,
    SHAZAM_LYRICS_WEB_ELEMENT_NAME,
)
from data_collectors.contract import ILyricsCollector
from data_collectors.tools import WebElementsExtractor
from data_collectors.utils.playwright import get_page_content


class ShazamLyricsCollector(ILyricsCollector):
    def __init__(
        self,
        pool_executor: AioPoolExecutor,
        web_elements_extractor: WebElementsExtractor = WebElementsExtractor(),
    ):
        self._pool_executor = pool_executor
        self._web_elements_extractor = web_elements_extractor

    async def collect(self, ids: List[str]) -> Dict[str, List[str]]:
        logger.info(f"Starting to collect {len(ids)} tracks lyrics from Shazam")

        async with async_playwright() as p:
            browser = await p.chromium.launch()
            results = await self._pool_executor.run(
                iterable=ids,
                func=partial(self._collect_single_track_lyrics, browser),
                expected_type=tuple,
            )

        return dict(results)

    async def _collect_single_track_lyrics(self, browser: Browser, track_id: str) -> Optional[Tuple[str, List[str]]]:
        page_source = await self._fetch_lyrics_page(browser, track_id)
        lyrics = self._extract_track_lyrics(page_source, track_id)

        if lyrics:
            return track_id, lyrics

    @staticmethod
    async def _fetch_lyrics_page(browser: Browser, track_id: str) -> Optional[str]:
        url = SHAZAM_TRACK_URL_FORMAT.format(track_id=track_id)
        page = await browser.new_page()
        await page.goto(url)

        return await get_page_content(page)

    def _extract_track_lyrics(self, page_source: Optional[str], track_id: str) -> Optional[List[str]]:
        if page_source is None:
            return

        web_elements = self._web_elements_extractor.extract(page_source, SHAZAM_LYRICS_WEB_ELEMENT)

        if web_elements:
            lyrics = web_elements[0][SHAZAM_LYRICS_WEB_ELEMENT_NAME]
            return [row.strip() for row in lyrics.split("\n")]

        logger.warn(f"Did not find lyrics for track `{track_id}`")
