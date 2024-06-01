from typing import List, Optional

from spotipyio import SearchItem, SearchItemMetadata, SpotifySearchType, MatchingEntity

from data_collectors.logic.collectors.charts.charts_searchers.base_chart_key_searcher import BaseChartKeySearcher
from data_collectors.utils.charts import extract_artist_and_track_from_chart_key


class IsraeliChartKeySearcher(BaseChartKeySearcher):
    async def _build_search_item(self, key: str) -> Optional[SearchItem]:
        return SearchItem(
            text=key,
            metadata=SearchItemMetadata(
                search_types=[SpotifySearchType.TRACK],
                quote=False
            )
        )

    def _build_matching_entities_options(self, search_item: SearchItem) -> List[MatchingEntity]:
        token_a, token_b = extract_artist_and_track_from_chart_key(search_item.text)
        entity = MatchingEntity(
            track=token_a.strip(),
            artist=token_b.strip()
        )
        reversed_entity = MatchingEntity(
            track=token_b.strip(),
            artist=token_a.strip()
        )

        return [entity, reversed_entity]
