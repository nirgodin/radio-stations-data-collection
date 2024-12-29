from typing import Dict, Optional

from aiohttp import ClientSession
from genie_common.tools import AioPoolExecutor
from genie_common.utils import safe_nested_get

from data_collectors.consts.genius_consts import RESPONSE, RESULT, GENIUS_SEARCH_URL
from data_collectors.consts.shazam_consts import HITS
from data_collectors.consts.spotify_consts import ID
from data_collectors.contract import BaseSearchCollector
from data_collectors.logic.models import MissingTrack
from data_collectors.tools import MultiEntityMatcher
from data_collectors.utils.genius import is_valid_response


class GeniusSearchCollector(BaseSearchCollector):
    def __init__(
        self,
        session: ClientSession,
        pool_executor: AioPoolExecutor,
        entity_matcher: MultiEntityMatcher,
    ):
        super().__init__(pool_executor)
        self._session = session
        self._entity_matcher = entity_matcher

    async def _search_single_track(
        self, missing_track: MissingTrack
    ) -> Dict[MissingTrack, Optional[str]]:
        params = self._to_query(missing_track)
        response = await self._get(params)

        if is_valid_response(response):
            track_id = self._serialize_response(response, missing_track)
        else:
            track_id = None

        return {missing_track: track_id}

    def _to_query(self, missing_track: MissingTrack) -> Dict[str, str]:
        query = f"{missing_track.artist_name} - {missing_track.track_name}"
        return {"q": query}

    async def _get(self, params: Dict[str, str]) -> dict:
        async with self._session.get(
            url=GENIUS_SEARCH_URL, params=params
        ) as raw_response:
            raw_response.raise_for_status()
            return await raw_response.json()

    def _serialize_response(
        self, response: dict, missing_track: MissingTrack
    ) -> Optional[str]:
        hits = safe_nested_get(response, [RESPONSE, HITS])

        if hits:
            return self._entity_matcher.match(
                entity=missing_track.to_matching_entity(),
                prioritized_candidates=hits,
                extract_fn=self._extract_genius_id,
            )

    @staticmethod
    def _extract_genius_id(hit: Dict[str, dict]) -> Optional[str]:
        track_id = safe_nested_get(hit, [RESULT, ID])
        return None if track_id is None else str(track_id)
