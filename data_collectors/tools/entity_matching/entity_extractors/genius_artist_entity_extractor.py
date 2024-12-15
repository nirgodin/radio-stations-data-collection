from typing import Optional, Dict

from genie_common.utils import safe_nested_get
from spotipyio.tools.extractors import IEntityExtractor

from data_collectors.consts.genius_consts import RESULT, PRIMARY_ARTIST
from data_collectors.consts.spotify_consts import NAME


class GeniusArtistEntityExtractor(IEntityExtractor):
    def extract(self, entity: Dict[str, dict]) -> Optional[str]:
        return safe_nested_get(entity, [RESULT, PRIMARY_ARTIST, NAME])

    @property
    def name(self) -> str:
        return "artist"
