from abc import ABC, abstractmethod
from typing import List, Optional

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
        search_item = self._build_search_item(chart_entry.key)
        search_result = await self._spotify_client.search.run_single(search_item)
        track = self._extract_matching_track(chart_entry, search_result)

        if track is not None:
            chart_entry.track_id = track[ID]
            track = {TRACK: track}

        return RadioChartEntryDetails(
            entry=chart_entry,
            track=track
        )

    @abstractmethod
    def _build_search_item(self, key: str) -> SearchItem:
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
