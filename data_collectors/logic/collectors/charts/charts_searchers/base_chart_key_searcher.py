from abc import ABC, abstractmethod
from typing import List, Optional

from async_lru import alru_cache
from genie_common.tools import logger
from genie_common.utils import safe_nested_get
from genie_datastores.postgres.models import ChartEntry
from spotipyio import SpotifyClient, EntityMatcher, SearchItem, MatchingEntity

from data_collectors.consts.spotify_consts import TRACKS, ITEMS, ID, TRACK
from data_collectors.logic.models import RadioChartEntryDetails


class BaseChartKeySearcher(ABC):
    def __init__(self, spotify_client: SpotifyClient, entity_matcher: EntityMatcher = EntityMatcher()):
        self._spotify_client = spotify_client
        self._entity_matcher = entity_matcher

    async def search(self, chart_entry: ChartEntry) -> RadioChartEntryDetails:
        track = await self._search_track_by_key(chart_entry.key)

        if track is not None:
            chart_entry.track_id = track[ID]
            track = {TRACK: track}

        return RadioChartEntryDetails(
            entry=chart_entry,
            track=track
        )

    @alru_cache(maxsize=1000)
    async def _search_track_by_key(self, key: str) -> Optional[dict]:
        search_item = await self._build_search_item(key)
        search_result = await self._spotify_client.search.run_single(search_item)

        return self._extract_matching_track(search_item, search_result)

    @abstractmethod
    async def _build_search_item(self, key: str) -> SearchItem:
        raise NotImplementedError

    def _extract_matching_track(self, search_item: SearchItem, search_result: dict) -> Optional[dict]:
        items = safe_nested_get(search_result, [TRACKS, ITEMS], [])
        matching_entities = self._build_matching_entities_options(search_item)

        for candidate in items:
            for entity in matching_entities:
                is_matching, score = self._entity_matcher.match(entity, candidate)

                if is_matching:
                    return candidate

        logger.info(f"Did not find any track that matches . Ignoring")

    @abstractmethod
    def _build_matching_entities_options(self, search_item: SearchItem) -> List[MatchingEntity]:
        raise NotImplementedError
