from typing import Optional, Dict

from genie_common.utils import safe_nested_get
from spotipyio.contract import IEntityExtractor

from data_collectors.consts.genius_consts import RESULT
from data_collectors.consts.shazam_consts import TITLE


class GeniusTrackEntityExtractor(IEntityExtractor):
    def extract(self, entity: Dict[str, dict]) -> Optional[str]:
        return safe_nested_get(entity, [RESULT, TITLE])

    @property
    def name(self) -> str:
        return "track"
