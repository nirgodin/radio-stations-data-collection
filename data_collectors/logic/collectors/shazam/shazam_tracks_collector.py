from typing import Any, List

from data_collectors.contract.collector_interface import ICollector
from data_collectors.logic.collectors.shazam.base_shazam_collector import BaseShazamCollector


class ShazamTracksCollector(BaseShazamCollector):
    async def collect(self, ids: List[str]) -> List[dict]:
        return await self._pool_executor.run(
            iterable=ids,
            func=self._shazam.track_about
        )
