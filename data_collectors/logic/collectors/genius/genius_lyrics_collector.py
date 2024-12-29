from typing import List, Tuple, Optional, Dict

from aiohttp import ClientSession, ClientResponse
from genie_common.tools import AioPoolExecutor, logger
from genie_common.utils import sub_all_digits

from data_collectors.consts.genius_consts import (
    GENIUS_LYRICS_URL_FORMAT,
    GENIUS_LYRICS_WEB_ELEMENT,
    GENIUS_LYRICS_ELEMENT_NAME,
    INVALID_LYRICS_ROWS,
)
from data_collectors.contract import ILyricsCollector
from data_collectors.tools import WebElementsExtractor


class GeniusLyricsCollector(ILyricsCollector):
    def __init__(
        self,
        session: ClientSession,
        pool_executor: AioPoolExecutor,
        web_elements_extractor: WebElementsExtractor = WebElementsExtractor(),
    ):
        self._session = session
        self._pool_executor = pool_executor
        self._web_elements_extractor = web_elements_extractor

    async def collect(self, ids: List[str]) -> Dict[str, List[str]]:
        logger.info(f"Starting to collect lyrics from Genius for {len(ids)} tracks")
        results = await self._pool_executor.run(
            iterable=ids, func=self._collect_single_track_lyrics, expected_type=tuple
        )

        return dict(results)

    async def _collect_single_track_lyrics(
        self, track_id: str
    ) -> Optional[Tuple[str, List[str]]]:
        url = GENIUS_LYRICS_URL_FORMAT.format(id=track_id)

        async with self._session.get(url) as raw_response:
            if raw_response.ok:
                return await self._serialize_response(track_id, raw_response)
            else:
                logger.warn(
                    f"Did not find lyrics page for track `{track_id}`. Ignoring"
                )

    async def _serialize_response(
        self, track_id: str, raw_response: ClientResponse
    ) -> Optional[Tuple[str, List[str]]]:
        page_content = await raw_response.text()
        raw_html_elements = self._web_elements_extractor.extract(
            html=page_content, web_element=GENIUS_LYRICS_WEB_ELEMENT
        )
        raw_lyrics = self._extract_lyrics_from_html(raw_html_elements)

        if raw_lyrics is None:
            logger.warn(
                f"Was not able to extract lyrics from page for track `{track_id}`. Ignoring"
            )
        else:
            lyrics = [row for row in raw_lyrics if self._is_valid_row(row)]
            return track_id, lyrics

    @staticmethod
    def _extract_lyrics_from_html(
        raw_html_elements: Optional[List[Dict[str, str]]]
    ) -> Optional[str]:
        if raw_html_elements:
            return raw_html_elements[0][GENIUS_LYRICS_ELEMENT_NAME]

    @staticmethod
    def _is_valid_row(row: str) -> bool:
        non_digits_row = sub_all_digits(row)
        return non_digits_row.lower().strip() not in INVALID_LYRICS_ROWS
