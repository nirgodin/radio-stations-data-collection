from typing import List, Dict, Tuple

from genie_common.tools import AioPoolExecutor, logger
from playwright.async_api import Browser

from data_collectors.consts.spotify_consts import (
    SPOTIFY_OPEN_ARTIST_URL_FORMAT,
    SPOTIFY_INFOBOX_SELECTOR,
)
from data_collectors.contract import ICollector
from data_collectors.logic.models import WebElement, HTMLElement, SpotifyArtistAbout
from data_collectors.logic.serializers import SpotifyArtistAboutSerializer
from data_collectors.tools import WebElementsExtractor
from data_collectors.utils.playwright import get_page_content


class SpotifyArtistsAboutCollector(ICollector):
    def __init__(
        self,
        pool_executor: AioPoolExecutor,
        browser: Browser,
        web_elements_extractor: WebElementsExtractor = WebElementsExtractor(),
        artist_about_serializer: SpotifyArtistAboutSerializer = SpotifyArtistAboutSerializer(),
    ):
        self._pool_executor = pool_executor
        self._browser = browser
        self._web_elements_extractor = web_elements_extractor
        self._artist_about_serializer = artist_about_serializer

    async def collect(self, id_name_map: Dict[str, str]) -> List[SpotifyArtistAbout]:
        if not id_name_map:
            logger.warning("Did not receive any artist id to collect. Returning empty list")
            return []

        logger.info(f"Starting to collect Spotify artists about for {len(id_name_map)} artists")
        return await self._pool_executor.run(
            iterable=id_name_map.items(),
            func=self._collect_single_artist_details_wrapper,
            expected_type=SpotifyArtistAbout,
        )

    async def _collect_single_artist_details_wrapper(self, artist_id_and_name: Tuple[str, str]) -> SpotifyArtistAbout:
        artist_id, artist_name = artist_id_and_name

        try:
            return await self._collect_single_artist_details(artist_id=artist_id, artist_name=artist_name)

        except Exception:
            logger.exception("Received exception during artist details collection. Returning empty details by default")
            return SpotifyArtistAbout(id=artist_id, name=artist_name)

    async def _collect_single_artist_details(self, artist_id: str, artist_name: str):
        html = await self._get_page_content(artist_id)
        details = []

        for element in self._web_elements:
            detail = self._web_elements_extractor.extract(html, element)
            details.extend(detail)

        return self._artist_about_serializer.serialize(artist_id, artist_name, details)

    async def _get_page_content(self, artist_id: str):
        url = SPOTIFY_OPEN_ARTIST_URL_FORMAT.format(id=artist_id)
        page = await self._browser.new_page()
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
                    name="social_links",
                    type=HTMLElement.A,
                    class_="muHL0_3HjlqTZDoapgc9",
                    multiple=True,
                ),
            ),
            WebElement(
                name="artist_infobox",
                type=HTMLElement.DIV,
                class_="TV2j1oIRIkKH_6D1xP82",
                child_element=WebElement(
                    name="about",
                    type=HTMLElement.P,
                    class_="Type__TypeElement-sc-goli3j-0",
                    multiple=True,
                    enumerate=False,
                ),
            ),
        ]
