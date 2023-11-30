from abc import ABC, abstractmethod
from typing import Any

from aiohttp import ClientSession

from data_collectors.consts.genius_consts import GENIUS_BASE_URL, META, STATUS
from data_collectors.contract import ICollector
from genie_common.utils import safe_nested_get


class BaseGeniusCollector(ICollector, ABC):
    def __init__(self, session: ClientSession):
        self._session = session

    async def _get(self, params: dict) -> Any:
        async with self._session.get(url=self._url, params=params) as raw_response:
            raw_response.raise_for_status()
            return await raw_response.json()

    @staticmethod
    def _is_valid_response(response: dict) -> bool:
        status_code = safe_nested_get(response, [META, STATUS])
        return status_code == 200

    @property
    @abstractmethod
    def _route(self) -> str:
        raise NotImplementedError

    @property
    def _url(self) -> str:
        return f"{GENIUS_BASE_URL}/{self._route}"
