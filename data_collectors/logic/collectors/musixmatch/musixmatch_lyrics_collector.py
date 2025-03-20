from typing import List, Dict, Tuple

from genie_common.utils import safe_nested_get

from data_collectors.contract import ILyricsCollector
from data_collectors.logic.collectors.musixmatch.base_musixmatch_collector import (
    BaseMusixmatchCollector,
)

MUSIXMATCH_LYRICS_END_SIGN = "..."


class MusixmatchLyricsCollector(BaseMusixmatchCollector, ILyricsCollector):
    async def collect(self, ids: List[str]) -> Dict[str, List[str]]:
        results = await self._pool_executor.run(
            iterable=ids, func=self._collect_single_track_lyrics, expected_type=tuple
        )
        return dict(results)

    async def _collect_single_track_lyrics(
        self, track_id: str
    ) -> Tuple[str, List[str]]:
        response = await self._get(params={"track_id": track_id})
        lyrics = safe_nested_get(response, ["message", "body", "lyrics", "lyrics_body"])

        if lyrics is not None:
            serialized_lyrics = self._serialize_lyrics(lyrics)
            return track_id, serialized_lyrics

    @staticmethod
    def _serialize_lyrics(raw_lyrics: str) -> List[str]:
        lyrics = []

        for row in raw_lyrics.split("\n"):
            formatted_row = row.strip()

            if formatted_row == MUSIXMATCH_LYRICS_END_SIGN:
                break

            if formatted_row != "":
                lyrics.append(formatted_row)

        return lyrics

    @property
    def _route(self) -> str:
        return "track.lyrics.get"
