from typing import Any, Optional

from genie_common.utils import safe_nested_get
from spotipyio.contract import IEntityExtractor

from data_collectors.consts.musixmatch_consts import TRACK_NAME
from data_collectors.consts.spotify_consts import TRACK


class MusixmatchTrackEntityExtractor(IEntityExtractor):
    def extract(self, entity: Any) -> Optional[str]:
        return safe_nested_get(entity, [TRACK, TRACK_NAME])

    @property
    def name(self) -> str:
        return "track"
