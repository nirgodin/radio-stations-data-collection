from functools import partial
from typing import Any, List, Dict

from genie_common.tools import AioPoolExecutor
from genie_common.utils import merge_dicts
from playwright.async_api import async_playwright, Browser

from data_collectors.consts.spotify_consts import SPOTIFY_OPEN_ARTIST_URL_FORMAT, SPOTIFY_INFOBOX_SELECTOR
from data_collectors.contract import ICollector
from data_collectors.logic.models import WebElement, HTMLElement, SpotifyArtistAbout
from data_collectors.logic.serializers import SpotifyArtistAboutSerializer
from data_collectors.tools import WebElementsExtractor
from data_collectors.utils.playwright import get_page_content


class SpotifyArtistsAboutCollector(ICollector):
    def __init__(self,
                 pool_executor: AioPoolExecutor,
                 web_elements_extractor: WebElementsExtractor = WebElementsExtractor(),
                 artist_about_serializer: SpotifyArtistAboutSerializer = SpotifyArtistAboutSerializer()):
        self._pool_executor = pool_executor
        self._web_elements_extractor = web_elements_extractor
        self._artist_about_serializer = artist_about_serializer

    async def collect(self, ids: List[str]) -> List[SpotifyArtistAbout]:
        async with async_playwright() as p:
            browser = await p.chromium.launch()

            return await self._pool_executor.run(
                iterable=ids,
                func=partial(self._collect_single_artist_details, browser),
                expected_type=SpotifyArtistAbout
            )

    async def _collect_single_artist_details(self, browser: Browser, artist_id: str) -> SpotifyArtistAbout:
        html = await self._get_page_content(browser, artist_id)
        details = []

        for element in self._web_elements:
            detail = self._web_elements_extractor.extract(html, element)
            details.extend(detail)

        return self._artist_about_serializer.serialize(artist_id, details)

    @staticmethod
    async def _get_page_content(browser: Browser, artist_id: str):
        url = SPOTIFY_OPEN_ARTIST_URL_FORMAT.format(id=artist_id)
        page = await browser.new_page()
        await page.goto(url)
        await page.locator(SPOTIFY_INFOBOX_SELECTOR).get_by_role("button").click()

        return await get_page_content(page)

    @property
    def _web_elements(self) -> List[WebElement]:
        return [
            WebElement(
                name="artist_infobox",
                type=HTMLElement.DIV,
                class_="T_AmQPlZ6wvE819I7A0D",
                child_element=WebElement(
                    name='social_links',
                    type=HTMLElement.A,
                    class_='muHL0_3HjlqTZDoapgc9',
                    multiple=True
                )
            ),
            WebElement(
                name='artist_infobox',
                type=HTMLElement.DIV,
                class_='TV2j1oIRIkKH_6D1xP82',
                child_element=WebElement(
                    name="about",
                    type=HTMLElement.P,
                    class_="Type__TypeElement-sc-goli3j-0 kmAjkQ",
                    multiple=True,
                    enumerate=False
                )
            ),
        ]
