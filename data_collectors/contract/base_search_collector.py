from abc import ABC, abstractmethod
from typing import Any, List, Dict, Optional

from data_collectors.contract.collector_interface import ICollector
from data_collectors.logic.models import MissingTrack
from genie_common.utils import merge_dicts
from genie_common.tools import logger
from data_collectors.tools import AioPoolExecutor


class BaseSearchCollector(ICollector, ABC):
    def __init__(self, pool_executor: AioPoolExecutor):
        self._pool_executor = pool_executor

    async def collect(self, missing_tracks: List[MissingTrack]) -> Dict[MissingTrack, Optional[str]]:
        logger.info(f"Starting to search for {len(missing_tracks)} queries")
        results = await self._pool_executor.run(
            iterable=missing_tracks,
            func=self._search_single_track,
            expected_type=dict
        )

        return merge_dicts(*results)

    @abstractmethod
    async def _search_single_track(self, missing_track: MissingTrack) -> Dict[MissingTrack, Optional[str]]:
        raise NotImplementedError

    @abstractmethod
    def _to_query(self, missing_track: MissingTrack) -> Any:
        raise NotImplementedError
