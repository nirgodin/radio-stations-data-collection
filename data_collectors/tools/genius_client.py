from typing import Any, Dict, Optional

from aiohttp import ClientSession

from data_collectors.consts.genius_consts import TEXT_FORMAT
from data_collectors.logic.models import GeniusTextFormat


class GeniusClient:
    def __init__(self, session: ClientSession, public_base_url: str, api_base_url: str, bearer_token: str):
        self._session = session
        self._public_base_url = public_base_url.rstrip("/")
        self._api_base_url = api_base_url.rstrip("/")
        self._bearer_token = bearer_token

    async def search_artist(self, name: str) -> Dict[str, Any]:
        url = f"{self._public_base_url}/search/artist"
        params = {"q": name}

        async with self._session.get(url, params=params) as raw_response:
            raw_response.raise_for_status()
            return await raw_response.json()

    async def get_artist(self, id_: str, text_format: GeniusTextFormat) -> Optional[Dict[str, Any]]:
        url = f"{self._api_base_url}/artists/{id_}"
        params = {TEXT_FORMAT: text_format.value}
        headers = {"Authorization": f"Bearer {self._bearer_token}"}

        async with self._session.get(url, params=params, headers=headers) as raw_response:
            raw_response.raise_for_status()
            return await raw_response.json()
