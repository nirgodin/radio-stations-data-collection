from typing import Any, Dict

from aiohttp import ClientSession


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
