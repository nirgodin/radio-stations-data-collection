from typing import Dict, Optional

from aiohttp import ClientSession

from data_collectors.consts.genius_consts import RESPONSE, RESULT
from data_collectors.consts.shazam_consts import HITS
from data_collectors.consts.spotify_consts import ID
from data_collectors.contract import BaseSearchCollector
from data_collectors.logic.collectors.genius.base_genius_collector import BaseGeniusCollector
from data_collectors.logic.models import MissingTrack
from genie_common.utils import safe_nested_get
from genie_common.tools import AioPoolExecutor

from data_collectors.tools import MultiEntityMatcher


class GeniusSearchCollector(BaseSearchCollector, BaseGeniusCollector):
    def __init__(self, session: ClientSession, pool_executor: AioPoolExecutor, entity_matcher: MultiEntityMatcher):
        super(BaseSearchCollector, self).__init__(session)
        super(BaseGeniusCollector, self).__init__()
        self._pool_executor = pool_executor
        self._entity_matcher = entity_matcher

    async def _search_single_track(self, missing_track: MissingTrack) -> Dict[MissingTrack, Optional[str]]:
        params = self._to_query(missing_track)
        response = await self._get(params)

        if self._is_valid_response(response):
            track_id = self._serialize_response(response, missing_track)
        else:
            track_id = None

        return {missing_track: track_id}

    def _serialize_response(self, response: dict, missing_track: MissingTrack) -> Optional[str]:
        hits = safe_nested_get(response, [RESPONSE, HITS])

        if hits:
            return self._entity_matcher.match(
                entity=missing_track.to_matching_entity(),
                prioritized_candidates=hits,
                extract_fn=self._extract_genius_id
            )

    @staticmethod
    def _extract_genius_id(hit: Dict[str, dict]) -> Optional[str]:
        track_id = safe_nested_get(hit, [RESULT, ID])
        return None if track_id is None else str(track_id)

    def _to_query(self, missing_track: MissingTrack) -> Dict[str, str]:
        query = f"{missing_track.artist_name} - {missing_track.track_name}"
        return {"q": query}

    @property
    def _route(self) -> str:
        return "search"
