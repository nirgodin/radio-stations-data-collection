from typing import List, Dict, Any, Union, Optional

from aiohttp import ClientSession
from genie_common.tools import logger


class JosieClient:
    def __init__(self, session: ClientSession, base_url: str):
        self._session = session
        self._base_url = base_url

    async def get_curators(self) -> List[Dict[str, Any]]:
        logger.info("Querying Josie curators")
        url = f"{self._base_url}/user/proposed-curators"
        response = await self._get(url)
        curators = response["data"]
        logger.info(f"Successfully retrieved {len(curators)} curators")

        return curators

    async def get_curator_posts(self, clerk_id: str, page: str = 1, limit: int = 100) -> List[Dict[str, Any]]:
        logger.info(f"Querying curator `{clerk_id}` posts")
        url = f"{self._base_url}/posts"
        params = {"authorId": clerk_id, "page": page, "limit": limit}
        response = await self._get(url, params)
        posts = response["data"]
        logger.info(f"Successfully retrieved {len(posts)} posts from curator `{clerk_id}`")

        return posts

    async def _get(self, url: str, params: Optional[dict] = None) -> Union[dict, list]:
        async with self._session.get(url=url, params=params) as raw_response:
            raw_response.raise_for_status()
            return await raw_response.json()
