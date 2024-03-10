from typing import Optional, Dict

from genie_common.utils import safe_nested_get
from spotipyio.contract import IEntityExtractor

from data_collectors.consts.shazam_consts import HEADING, TITLE


class ShazamTrackEntityExtractor(IEntityExtractor):
    def extract(self, entity: Dict[str, dict]) -> Optional[str]:
        return safe_nested_get(entity, [HEADING, TITLE])

    @property
    def name(self) -> str:
        return "track"
