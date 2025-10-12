import asyncio
from typing import Any, Dict, List

from aiohttp import ClientSession
from genie_common.tools import AioPoolExecutor
from genie_common.utils import chain_lists
from spotipyio.logic.utils import safe_nested_get

from data_collectors.contract import ICollector
from data_collectors.tools.josie_client import JosieClient


class JosieTracksCollector(ICollector):
    def __init__(self, josie_client: JosieClient, pool_executor: AioPoolExecutor = AioPoolExecutor()):
        self._josie_client = josie_client
        self._pool_executor = pool_executor

    async def collect(self) -> List[str]:
        curators = await self._josie_client.get_curators()
        tracks = await self._pool_executor.run(
            iterable=curators[:3], func=self._fetch_curator_posts, expected_type=list
        )

        return chain_lists(tracks)

    async def _fetch_curator_posts(self, curator: Dict[str, Any]):
        posts = await self._josie_client.get_curator_posts(curator["clerkId"])
        tracks = []

        for post in posts:
            post_tracks = self._collect_single_post_tracks(post)
            tracks.extend(post_tracks)

        return tracks

    def _collect_single_post_tracks(self, post: Dict[str, Any]) -> List[str]:
        paragraphs = post["paragraphs"]

        for paragraph in paragraphs:
            if self._is_spotify_tracks_paragraph(paragraph):
                return self._extract_paragraph_tracks_ids(paragraph)

        return []

    @staticmethod
    def _is_spotify_tracks_paragraph(paragraph: Dict[str, Any]) -> bool:
        if paragraph["type"] == "audio":
            return safe_nested_get(paragraph, ["content", "provider"]) == "Spotify"

        return False

    @staticmethod
    def _extract_paragraph_tracks_ids(paragraph: Dict[str, Any]) -> List[str]:
        tracks = safe_nested_get(paragraph, ["content", "tracks"])
        return [track["id"] for track in tracks]


async def main():
    async with ClientSession() as session:
        josie_client = JosieClient(session)
        collector = JosieTracksCollector(josie_client)

        await collector.collect()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
