from functools import partial
from typing import Any, List, Optional

from genie_common.tools import AioPoolExecutor, logger
from genie_common.utils import safe_nested_get

from data_collectors.consts.genius_consts import (
    ARTIST,
    RESPONSE,
)
from data_collectors.contract import ICollector
from data_collectors.logic.models import GeniusTextFormat
from data_collectors.tools import GeniusClient
from data_collectors.utils.genius import is_valid_response


class GeniusArtistsCollector(ICollector):
    def __init__(self, pool_executor: AioPoolExecutor, genius_client: GeniusClient):
        self._pool_executor = pool_executor
        self._genius_client = genius_client

    async def collect(self, ids: List[str], text_format: GeniusTextFormat = GeniusTextFormat.PLAIN) -> Any:
        logger.info(f"Starting to collect {len(ids)} artists from Genius")
        return await self._pool_executor.run(
            iterable=ids,
            func=partial(self._collect_single_artist, text_format),
            expected_type=dict,
        )

    async def _collect_single_artist(self, text_format: GeniusTextFormat, artist_id: str) -> Optional[dict]:
        response = await self._genius_client.get_artist(artist_id, text_format)

        if is_valid_response(response):
            return safe_nested_get(response, [RESPONSE, ARTIST])
