from abc import ABC, abstractmethod

from aiohttp import ClientSession

from data_collectors.consts.musixmatch_consts import MUSIXMATCH_BASE_URL
from data_collectors.contract import ICollector
from genie_common.tools import AioPoolExecutor


class BaseMusixmatchCollector(ICollector, ABC):
    def __init__(
        self, session: ClientSession, pool_executor: AioPoolExecutor, api_key: str
    ):
        self._session = session
        self._pool_executor = pool_executor
        self._api_key = api_key

    @property
    @abstractmethod
    def _route(self) -> str:
        raise NotImplementedError

    async def _get(self, params: dict) -> dict:
        async with self._session.get(url=self._url, params=params) as raw_response:
            raw_response.raise_for_status()
            return await raw_response.json(content_type=None)

    @property
    def _url(self) -> str:
        return f"{MUSIXMATCH_BASE_URL}/{self._route}?apikey={self._api_key}"
