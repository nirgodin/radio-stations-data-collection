from typing import List, Tuple, Optional, Dict

from genie_common.tools import AioPoolExecutor, logger
from genie_common.utils import safe_nested_get

from data_collectors.consts.genius_consts import (
    GENIUS_LYRICS_WEB_ELEMENT,
    GENIUS_LYRICS_ELEMENT_NAME,
    RESPONSE,
    SONG,
    URL,
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

    async def collect(self, ids: List[str]) -> Dict[str, str]:
        logger.info(f"Starting to collect lyrics from Genius for {len(ids)} tracks")
        results = await self._pool_executor.run(
            iterable=ids, func=self._collect_single_track_lyrics, expected_type=tuple
        )

        return dict(results)

    async def _collect_single_track_lyrics(self, track_id: str) -> Optional[Tuple[str, str]]:
        song = await self._genius_client.get_song(id_=track_id, text_format=GeniusTextFormat.PLAIN)
        url = safe_nested_get(song, [RESPONSE, SONG, URL])

        if url:
            page = await self._genius_client.get_song_page(url)
            lyrics = self._serialize_response(page)
            return track_id, lyrics

        else:
            logger.warning(f"Did not find valid URL for track `{track_id}`. Skipping")

    def _serialize_response(self, page_content: str) -> Optional[str]:
        raw_html_elements = self._web_elements_extractor.extract(
            html=page_content, web_element=GENIUS_LYRICS_WEB_ELEMENT
        )

        if raw_html_elements:
            lines = [elem[GENIUS_LYRICS_ELEMENT_NAME] for elem in raw_html_elements]
            return "\n".join(lines)
