from typing import Tuple, Dict, Any, Optional, List

from genie_common.tools import AioPoolExecutor, logger

from data_collectors.contract import ICollector
from data_collectors.logic.models import Domain
from data_collectors.tools import GoogleSearchClient


class GoogleArtistsWebPagesCollector(ICollector):
    def __init__(self, google_search_client: GoogleSearchClient, pool_executor: AioPoolExecutor, domains: List[Domain]):
        self._google_search_client = google_search_client
        self._pool_executor = pool_executor
        self._domains = domains

    async def collect(self, ids_names_map: Dict[str, str]) -> Dict[str, Dict[Domain, str]]:
        logger.info(f"Searching Google search for {len(ids_names_map)} artists")
        results = await self._pool_executor.run(
            iterable=list(ids_names_map.items()), func=self._collect_single_artist_web_pages, expected_type=tuple
        )

        return dict(results)

    async def _collect_single_artist_web_pages(
        self, artist_id_and_name: Tuple[str, str]
    ) -> Tuple[str, Optional[Dict[Domain, str]]]:
        artist_id, artist_name = artist_id_and_name
        response = await self._google_search_client.search_single(query=f"{artist_name} musician")
        web_pages = self._extract_relevant_web_pages(response)

        return artist_id, web_pages

    def _extract_relevant_web_pages(self, response: Dict[str, Any]) -> Dict[Domain, str]:
        web_pages: Dict[Domain, str] = {}
        items = response.get("items", [])

        for item in items:
            domain, link = self._extract_single_item_link(item)

            if self._is_relevant_link(domain, link, web_pages):
                web_pages[domain] = link

        return web_pages

    def _extract_single_item_link(self, item: Dict[str, Any]) -> Optional[Tuple[Optional[Domain], Optional[str]]]:
        link = item.get("link")

        if isinstance(link, str):
            lower_link = link.lower()

            for domain in self._domains:
                if lower_link.__contains__(domain.value):
                    return domain, link

        return None, None

    @staticmethod
    def _is_relevant_link(domain: Optional[Domain], link: Optional[str], existing_web_pages: Dict[Domain, str]) -> bool:
        return (link is not None) and (domain not in existing_web_pages.keys())
