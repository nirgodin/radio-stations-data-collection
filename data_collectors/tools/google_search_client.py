from typing import List, Any, Dict

from aiohttp import ClientSession
from genie_common.tools import AioPoolExecutor

from data_collectors.tools import GoogleSearchConfig


class GoogleSearchClient:
    def __init__(
        self,
        config: GoogleSearchConfig,
        session: ClientSession,
        pool_executor: AioPoolExecutor,
    ):
        self._config = config
        self._session = session
        self._pool_executor = pool_executor

    async def search(self, queries: List[str]) -> List[Dict[str, Any]]:
        return await self._pool_executor.run(
            iterable=queries,
            func=self.search_single,
            expected_type=dict,
        )

    async def search_single(self, query: str) -> Dict[str, Any]:
        params = {"key": self._config.api_key, "cx": self._config.cx, "q": query}

        async with self._session.get(url=self._config.base_url, params=params) as raw_response:
            raw_response.raise_for_status()
            return await raw_response.json()
