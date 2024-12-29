from http import HTTPStatus
from typing import Dict, Optional

from aiohttp import ClientSession
from genie_common.tools import AioPoolExecutor
from genie_common.utils import safe_nested_get

from data_collectors.consts.musixmatch_consts import (
    MESSAGE,
    BODY,
    TRACK_LIST,
    HEADER,
    STATUS_CODE,
    TRACK_ID,
)
from data_collectors.consts.spotify_consts import TRACK
from data_collectors.contract import BaseSearchCollector
from data_collectors.logic.collectors.musixmatch.base_musixmatch_collector import (
    BaseMusixmatchCollector,
)
from data_collectors.logic.models import MissingTrack
from data_collectors.tools import MultiEntityMatcher


class MusixmatchSearchCollector(BaseSearchCollector, BaseMusixmatchCollector):
    def __init__(
        self,
        session: ClientSession,
        pool_executor: AioPoolExecutor,
        api_key: str,
        entity_matcher: MultiEntityMatcher,
    ):
        super(BaseSearchCollector, self).__init__(session, pool_executor, api_key)
        super(BaseMusixmatchCollector, self).__init__()
        self._entity_matcher = entity_matcher

    async def _search_single_track(
        self, missing_track: MissingTrack
    ) -> Dict[MissingTrack, Optional[str]]:
        response = await self._get(params=self._to_query(missing_track))
        candidates = self._extract_candidates(response)
        track_id = self._entity_matcher.match(
            entity=missing_track.to_matching_entity(),
            prioritized_candidates=candidates,
            extract_fn=self._extract_track_id,
        )

        return {missing_track: track_id}

    @staticmethod
    def _extract_candidates(response: dict) -> list:
        status_code = safe_nested_get(response, [MESSAGE, HEADER, STATUS_CODE])

        if status_code == HTTPStatus.OK.value:
            return safe_nested_get(response, [MESSAGE, BODY, TRACK_LIST], default=[])

        return []

    @staticmethod
    def _extract_track_id(candidate: Dict[str, dict]) -> Optional[str]:
        track_id = safe_nested_get(candidate, [TRACK, TRACK_ID])

        if track_id is not None:
            return str(track_id)

    def _to_query(self, missing_track: MissingTrack) -> dict:
        return {
            "q_artist": missing_track.artist_name,
            "q_track": missing_track.track_name,
        }

    @property
    def _route(self) -> str:
        return "track.search"
