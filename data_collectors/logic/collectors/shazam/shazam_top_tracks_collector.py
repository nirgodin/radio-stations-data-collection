from typing import List, Dict, Optional

from genie_common.tools import logger
from genie_datastores.postgres.models import ShazamLocation
from playwright.async_api import Browser

from data_collectors.consts.shazam_consts import SHAZAM_TOP_TRACK_WEB_ELEMENT, SHAZAM_TOP_TRACKS_CSS_SELECTOR
from data_collectors.contract import ICollector
from data_collectors.tools import WebElementsExtractor
from data_collectors.utils.playwright import get_page_content


class ShazamTopTracksCollector(ICollector):
    def __init__(
        self,
        browser: Browser,
        web_elements_extractor: WebElementsExtractor = WebElementsExtractor(),
    ):
        self._browser = browser
        self._web_elements_extractor = web_elements_extractor

    async def collect(self) -> Dict[ShazamLocation, List[str]]:
        logger.info("Starting to execute shazam top tracks collection")
        results = {}

        for location, url in self._location_to_url_mapping.items():
            logger.info(f"Starting to collect tracks for `{location.value}`")
            tracks_ids = await self._collect_single_location_tracks(url)
            results[location] = tracks_ids

        logger.info("Successfully finished executing shazam top tracks collector")
        return results

    async def _collect_single_location_tracks(
        self,
        url: str,
    ) -> List[str]:
        page = await self._browser.new_page()
        await page.goto(url)
        await page.wait_for_selector(SHAZAM_TOP_TRACKS_CSS_SELECTOR)
        content = await get_page_content(page)
        results = self._web_elements_extractor.extract(content, SHAZAM_TOP_TRACK_WEB_ELEMENT)

        return self._extract_tracks_ids(results)

    def _extract_tracks_ids(self, results: List[Dict[str, str]]) -> List[str]:
        tracks_ids = []

        for result in results:
            track_id = self._extract_single_result_id(result)

            if track_id is not None and track_id not in tracks_ids:
                tracks_ids.append(track_id)

        return tracks_ids

    def _extract_single_result_id(self, result: Dict[str, str]) -> Optional[str]:
        for url in result.values():
            formatted_url = url.lstrip("/").lower()

            if formatted_url.startswith("song"):
                return self._extract_track_id(formatted_url)

    @staticmethod
    def _extract_track_id(url: str) -> Optional[str]:
        split_url = url.split("/")

        if len(split_url) >= 2:
            return split_url[1]

    @property
    def _location_to_url_mapping(self) -> Dict[ShazamLocation, str]:
        return {
            ShazamLocation.TEL_AVIV: "https://www.shazam.com/charts/top-50/israel/tel-aviv",
            ShazamLocation.ISRAEL: "https://www.shazam.com/charts/top-200/israel",
            ShazamLocation.WORLD: "https://www.shazam.com/charts/top-200/world",
        }
