from typing import Any, Dict, List, Optional, Tuple

from genie_common.tools import AioPoolExecutor, logger
from genie_common.utils import safe_nested_get
from spotipyio.models import MatchingEntity
from spotipyio.tools.matching import MultiEntityMatcher

from data_collectors.contract import ICollector
from data_collectors.tools import GeniusClient


class GeniusArtistsIDsCollector(ICollector):
    def __init__(self, genius_client: GeniusClient, pool_executor: AioPoolExecutor, entity_matcher: MultiEntityMatcher):
        self._genius_client = genius_client
        self._pool_executor = pool_executor
        self._entity_matcher = entity_matcher

    async def collect(self, artists_ids_to_names: Dict[str, str]) -> Dict[str, Optional[str]]:
        logger.info(f"Starting to search Genius for `{len(artists_ids_to_names)}` artists")
        results = await self._pool_executor.run(
            func=self._search_single_artist,
            iterable=artists_ids_to_names.items(),
            expected_type=tuple,
        )
        return dict(results)

    async def _search_single_artist(self, artist_id_and_name: Tuple[str, str]) -> Tuple[str, str]:
        artist_id, artist_name = artist_id_and_name
        search_response = await self._genius_client.search_artist(artist_name)
        genius_artist_id = self._extract_matching_artist_id(search_response, artist_name)

        return artist_id, genius_artist_id

    def _extract_matching_artist_id(self, response: Dict[str, Any], artist_name: str) -> Optional[str]:
        hits = self._extract_artist_hits(response)

        if not hits:
            return None

        matching_entity = MatchingEntity(artist=artist_name)
        candidate = self._entity_matcher.match(
            entities=[matching_entity],
            candidates=hits,
        )

        return None if candidate is None else self._extract_artist_id(candidate)

    @staticmethod
    def _extract_artist_hits(response: Dict[str, Any]) -> List[Dict[str, Any]]:
        sections = safe_nested_get(response, ["response", "sections"], [])

        for section in sections:
            if section.get("type") == "artist":
                return section.get("hits", [])

        return []

    @staticmethod
    def _extract_artist_id(hit: Dict[str, Any]) -> Optional[str]:
        artist_id = safe_nested_get(hit, ["result", "id"])
        return None if artist_id is None else str(artist_id)
