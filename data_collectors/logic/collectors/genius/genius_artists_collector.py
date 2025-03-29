from functools import partial
from typing import Any, List

from aiohttp import ClientSession
from genie_common.tools import AioPoolExecutor, logger
from genie_common.utils import safe_nested_get

from data_collectors.consts.genius_consts import (
    GENIUS_ARTIST_URL_FORMAT,
    TEXT_FORMAT,
    ARTIST,
    RESPONSE,
)
from data_collectors.contract import ICollector
from data_collectors.logic.models import GeniusTextFormat
from data_collectors.utils.genius import is_valid_response


class GeniusArtistsCollector(ICollector):
    def __init__(self, pool_executor: AioPoolExecutor, session: ClientSession):
        self._pool_executor = pool_executor
        self._session = session

    async def collect(self, ids: List[str], text_format: GeniusTextFormat = GeniusTextFormat.PLAIN) -> Any:
        logger.info(f"Starting to collect {len(ids)} artists from Genius")
        return await self._pool_executor.run(
            iterable=ids,
            func=partial(self._collect_single_artist, text_format),
            expected_type=dict,
        )

    async def _collect_single_artist(self, text_format: GeniusTextFormat, artist_id: str) -> dict:
        url = GENIUS_ARTIST_URL_FORMAT.format(id=artist_id)
        params = {TEXT_FORMAT: text_format.value}

        async with self._session.get(url=url, params=params) as raw_response:
            raw_response.raise_for_status()
            response = await raw_response.json()

        if is_valid_response(response):
            return safe_nested_get(response, [RESPONSE, ARTIST])
