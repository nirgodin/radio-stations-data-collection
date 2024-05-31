from typing import Optional, List

from genie_common.clients.google import GoogleTranslateClient
from genie_common.utils import contains_any_hebrew_character
from spotipyio import SpotifyClient, EntityMatcher, SearchItem, SearchItemFilters, SearchItemMetadata, \
    SpotifySearchType, MatchingEntity

from data_collectors.logic.collectors.charts.charts_searchers.base_chart_key_searcher import BaseChartKeySearcher
from data_collectors.utils.charts import extract_artist_and_track_from_chart_key


class ArtistTranslatorChartKeySearcher(BaseChartKeySearcher):
    def __init__(self,
                 spotify_client: SpotifyClient,
                 translation_client: GoogleTranslateClient,
                 entity_matcher: EntityMatcher):
        super().__init__(spotify_client, entity_matcher)
        self._translation_client = translation_client

    async def _build_search_item(self, key: str) -> SearchItem:
        artist, track = extract_artist_and_track_from_chart_key(key)

        if contains_any_hebrew_character(artist):
            artist = await self._translate_artist_name(artist)

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

    async def _translate_artist_name(self, artist: str) -> Optional[str]:  # TODO: Extract to translationAdapter
        translation_response = await self._translation_client.translate(
            texts=[artist],
            target_language="en",
            source_language="he"
        )

        if translation_response:
            first_translation = translation_response[0]
            return first_translation.translation
