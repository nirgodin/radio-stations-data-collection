from typing import List, Tuple, Optional, Dict

from genie_common.tools import AioPoolExecutor, logger
from genie_common.utils import sub_all_digits, safe_nested_get

from data_collectors.consts.genius_consts import (
    GENIUS_LYRICS_WEB_ELEMENT,
    GENIUS_LYRICS_ELEMENT_NAME,
    INVALID_LYRICS_ROWS, RESPONSE, SONG, URL,
)
from data_collectors.contract import ILyricsCollector
from data_collectors.logic.models import GeniusTextFormat
from data_collectors.tools import WebElementsExtractor, GeniusClient


class GeniusLyricsCollector(ILyricsCollector):
    def __init__(
        self,
        pool_executor: AioPoolExecutor,
        genius_client: GeniusClient,
        web_elements_extractor: WebElementsExtractor = WebElementsExtractor(),
    ):
        self._pool_executor = pool_executor
        self._genius_client = genius_client
        self._web_elements_extractor = web_elements_extractor

    async def collect(self, ids: List[str]) -> Dict[str, List[str]]:
        logger.info(f"Starting to collect lyrics from Genius for {len(ids)} tracks")
        results = await self._pool_executor.run(
            iterable=ids, func=self._collect_single_track_lyrics, expected_type=tuple
        )

        return dict(results)

    async def _collect_single_track_lyrics(self, track_id: str) -> Optional[Tuple[str, List[str]]]:
        song = await self._genius_client.get_song(id_=track_id, text_format=GeniusTextFormat.PLAIN)
        url = safe_nested_get(song, [RESPONSE, SONG, URL])

        if url:
            page = await self._genius_client.get_song_page(url)
            return self._serialize_response(track_id, page)
        else:
            logger.warning(f"Did not find valid URL for track `{track_id}`. Skipping")

    def _serialize_response(self, track_id: str, page_content: str) -> Optional[Tuple[str, List[str]]]:
        raw_html_elements = self._web_elements_extractor.extract(
            html=page_content, web_element=GENIUS_LYRICS_WEB_ELEMENT
        )
        raw_lyrics = self._extract_lyrics_from_html(raw_html_elements)

        if raw_lyrics is None:
            logger.warn(f"Was not able to extract lyrics from page for track `{track_id}`. Ignoring")
        else:
            lyrics = [row for row in raw_lyrics if self._is_valid_row(row)]
            return track_id, lyrics

    @staticmethod
    def _extract_lyrics_from_html(raw_html_elements: Optional[List[Dict[str, str]]]) -> Optional[List[str]]:
        if raw_html_elements:
            return [elem[GENIUS_LYRICS_ELEMENT_NAME] for elem in raw_html_elements]

    @staticmethod
    def _is_valid_row(row: str) -> bool:
        non_digits_row = sub_all_digits(row)
        return non_digits_row.lower().strip() not in INVALID_LYRICS_ROWS
