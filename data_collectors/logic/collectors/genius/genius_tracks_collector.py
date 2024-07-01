from functools import partial
from typing import Optional, Dict, List, Literal

from aiohttp import ClientSession
from genie_common.tools import AioPoolExecutor
from genie_common.utils import safe_nested_get

from data_collectors.consts.genius_consts import GENIUS_TRACK_URL_FORMAT, TEXT_FORMAT, RESPONSE, SONG
from data_collectors.contract import ICollector
from data_collectors.logic.models import GeniusTextFormat
from data_collectors.utils.genius import is_valid_response


class GeniusTracksCollector(ICollector):
    def __init__(self, pool_executor: AioPoolExecutor, session: ClientSession):
        self._pool_executor = pool_executor
        self._session = session

    async def collect(self, ids: List[str], text_format: GeniusTextFormat) -> List[dict]:
        return await self._pool_executor.run(
            iterable=ids,
            func=partial(self._collect_single, text_format),
            expected_type=dict
        )

    async def _collect_single(self, text_format: GeniusTextFormat, track_id: str) -> dict:
        url = GENIUS_TRACK_URL_FORMAT.format(id=track_id)
        params = {TEXT_FORMAT: text_format.value}

        async with self._session.get(url=url, params=params) as raw_response:
            raw_response.raise_for_status()
            response = await raw_response.json()

        if is_valid_response(response):
            return safe_nested_get(response, [RESPONSE, SONG])
