from typing import Optional

from aiohttp import ClientSession
from genie_common.clients.utils import jsonify_response

from data_collectors.consts.google_consts import ADDRESS, LANGUAGE


class RapidAPIClient:
    def __init__(self, session: ClientSession, api_key: str, api_host: str, base_url: str):
        self._session = session
        self._api_key = api_key
        self._api_host = api_host
        self._base_url = base_url

    async def geocode(self, address: str) -> Optional[dict]:
        url = f"{self._base_url}/maps/api/geocode/json"
        headers = {"x-rapidapi-key": self._api_key, "x-rapidapi-host": self._api_host}
        params = {ADDRESS: address, LANGUAGE: "en"}

        async with self._session.get(url=url, params=params, headers=headers) as raw_response:
            return await jsonify_response(raw_response)
