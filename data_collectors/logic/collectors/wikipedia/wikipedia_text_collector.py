from aiohttp import ClientSession

from data_collectors.contract import ICollector


class WikipediaTextCollector(ICollector):
    def __init__(self, session: ClientSession, base_url: str):
        self._session = session
        self._base_url = base_url.rstrip("/")

    async def collect(self, title: str) -> str:
        url = f"{self._base_url}/{title}"

        async with self._session.get(url) as raw_response:
            raw_response.raise_for_status()
            return await raw_response.text()
