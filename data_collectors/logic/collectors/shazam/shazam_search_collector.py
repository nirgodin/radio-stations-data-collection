from typing import Dict, Optional, List

from shazamio import Shazam
from spotipyio import EntityMatcher

from data_collectors.consts.shazam_consts import HITS, KEY, HEADING, TITLE, SUBTITLE
from data_collectors.consts.spotify_consts import TRACKS, NAME, ARTISTS
from data_collectors.contract import BaseSearchCollector
from data_collectors.logic.collectors.shazam.base_shazam_collector import BaseShazamCollector
from data_collectors.logic.models import MissingTrack
from genie_common.utils import safe_nested_get
from genie_common.tools import logger
from genie_common.tools import AioPoolExecutor


class ShazamSearchCollector(BaseSearchCollector, BaseShazamCollector):
    def __init__(self, shazam: Shazam, pool_executor: AioPoolExecutor, entity_matcher: EntityMatcher = EntityMatcher()):
        super(BaseShazamCollector, self).__init__()
        super(BaseSearchCollector, self).__init__(shazam, pool_executor)
        self._entity_matcher = entity_matcher

    def _to_query(self, missing_track: MissingTrack) -> str:
        return f"{missing_track.artist_name} - {missing_track.track_name}"

    async def _search_single_track(self, missing_track: MissingTrack) -> Dict[MissingTrack, Optional[str]]:
        query = self._to_query(missing_track)
        response = await self._shazam.search_track(query=query, limit=1)

        if isinstance(response, dict):
            track_id = self._extract_matching_track_id(response, missing_track)
        else:
            logger.info(f"Did not find any search result for query `{query}`")
            track_id = None

        return {missing_track: track_id}

    def _extract_matching_track_id(self, response: Dict[str, dict], missing_track: MissingTrack) -> Optional[str]:
        hits = safe_nested_get(response, [TRACKS, HITS])

        if hits:
            return self._match_hits(hits, missing_track)

    def _match_hits(self, hits: List[Dict[str, list]], missing_track: MissingTrack) -> Optional[str]:
        for hit in hits:
            candidate = self._to_spotify_format(hit)
            entity = missing_track.to_matching_entity()
            is_matching, score = self._entity_matcher.match(entity, candidate)

            if is_matching:
                return hit.get(KEY)

        logger.info("Search request returned hits, but failed to match any of them. Skipping")

    @staticmethod
    def _to_spotify_format(hit: Dict[str, list]) -> Dict[str, dict]:
        track_name = safe_nested_get(hit, [HEADING, TITLE])
        artist_name = safe_nested_get(hit, [HEADING, SUBTITLE])

        return {
            NAME: track_name,
            ARTISTS: [
                {
                    NAME: artist_name
                }
            ]
        }
