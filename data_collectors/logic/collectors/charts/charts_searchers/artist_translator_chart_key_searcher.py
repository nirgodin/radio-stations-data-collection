from typing import Optional, List

from genie_common.utils import contains_any_hebrew_character
from genie_datastores.models import EntityType
from spotipyio import SpotifyClient, EntityMatcher, SearchItem, SearchItemFilters, SearchItemMetadata, \
    SpotifySearchType, MatchingEntity

from data_collectors.logic.collectors.charts.charts_searchers.base_chart_key_searcher import BaseChartKeySearcher
from data_collectors.tools import TranslationAdapter
from data_collectors.utils.charts import extract_artist_and_track_from_chart_key


class ArtistTranslatorChartKeySearcher(BaseChartKeySearcher):
    def __init__(self,
                 spotify_client: SpotifyClient,
                 translation_adapter: TranslationAdapter,
                 entity_matcher: EntityMatcher):
        super().__init__(spotify_client, entity_matcher)
        self._translation_adapter = translation_adapter

    async def _build_search_item(self, key: str) -> Optional[SearchItem]:
        artist, track = extract_artist_and_track_from_chart_key(key)

        if contains_any_hebrew_character(artist):
            artist = await self._translation_adapter.translate(
                text=artist,
                target_language="en",
                source_language="he",
                entity_type=EntityType.ARTIST
            )

        if artist is not None:
            return SearchItem(
                filters=SearchItemFilters(
                    track=track,
                    artist=artist
                ),
                metadata=SearchItemMetadata(
                    search_types=[SpotifySearchType.TRACK]
                )
            )

    def _build_matching_entities_options(self, search_item: SearchItem) -> List[MatchingEntity]:
        return [MatchingEntity(artist=search_item.filters.artist, track=search_item.filters.track)]
