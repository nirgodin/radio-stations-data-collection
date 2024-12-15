from typing import Optional, Dict

from genie_common.utils import safe_nested_get
from spotipyio.tools.extractors import IEntityExtractor

from data_collectors.consts.shazam_consts import HEADING, SUBTITLE


class ShazamArtistEntityExtractor(IEntityExtractor):
    def extract(self, entity: Dict[str, dict]) -> Optional[str]:
        return safe_nested_get(entity, [HEADING, SUBTITLE])

    @property
    def name(self) -> str:
        return "artist"
