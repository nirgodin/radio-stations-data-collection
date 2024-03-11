from typing import Any, Optional, Dict

from genie_common.utils import safe_nested_get
from spotipyio.contract import IEntityExtractor

from data_collectors.consts.musixmatch_consts import ARTIST_NAME
from data_collectors.consts.spotify_consts import TRACK


class MusixmatchArtistEntityExtractor(IEntityExtractor):
    def extract(self, entity: Dict[str, dict]) -> Optional[str]:
        return safe_nested_get(entity, [TRACK, ARTIST_NAME])

    @property
    def name(self) -> str:
        return "artist"
