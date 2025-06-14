from typing import Tuple, Dict, Any

from genie_common.tools import AioPoolExecutor, logger

from data_collectors.contract import ICollector
from data_collectors.tools import GoogleSearchClient


class GoogleArtistsWebPagesCollector(ICollector):
    def __init__(self, google_search_client: GoogleSearchClient, pool_executor: AioPoolExecutor):
        self._google_search_client = google_search_client
        self._pool_executor = pool_executor

    async def collect(self, ids_names_map: Dict[str, str]) -> Dict[str, Dict[str, str]]:
        logger.info(f"Searching Google search for {len(ids_names_map)} artists")
        results = await self._pool_executor.run(
            iterable=list(ids_names_map.items()), func=self._collect_single_artist_web_pages, expected_type=tuple
        )

        return dict(results)

    async def _collect_single_artist_web_pages(self, artist_id_and_name: Tuple[str, str]) -> Tuple[str, Dict[str, str]]:
        artist_id, artist_name = artist_id_and_name
        response = await self._google_search_client.search_single(query=f"{artist_name} musician")
        web_pages = self._extract_relevant_web_pages(response)

        return artist_id, web_pages

    def _extract_relevant_web_pages(self, response: Dict[str, Any]) -> Dict[str, str]:
        print("b")
