from typing import Optional, Dict

from genie_common.utils import safe_nested_get
from spotipyio.tools.extractors import IEntityExtractor

from data_collectors.consts.genius_consts import RESULT
from data_collectors.consts.spotify_consts import NAME


class GeniusSearchResultArtistEntityExtractor(IEntityExtractor):
    def extract(self, entity: Dict[str, dict]) -> Optional[str]:
        return safe_nested_get(entity, [RESULT, NAME])

    @property
    def name(self) -> str:
        return "artist"
