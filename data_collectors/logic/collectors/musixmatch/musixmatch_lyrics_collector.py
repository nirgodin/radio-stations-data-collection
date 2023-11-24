from typing import List

from data_collectors.logic.collectors.musixmatch.base_musixmatch_collector import BaseMusixmatchCollector


class MusixmatchLyricsCollector(BaseMusixmatchCollector):
    async def collect(self, ids: List[str]) -> List[dict]:
        return await self._pool_executor.run(
            iterable=ids,
            func=self._collect_single_track_lyrics,
            expected_type=dict
        )

    async def _collect_single_track_lyrics(self, track_id: str) -> dict:
        return await self._get(params={"track_id": track_id})

    @property
    def _route(self) -> str:
        return "track.lyrics.get"
