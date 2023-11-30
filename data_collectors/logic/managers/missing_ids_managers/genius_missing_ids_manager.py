from typing import Dict, Optional

from genie_datastores.postgres.models import TrackIDMapping

from data_collectors.logic.managers.missing_ids_managers.base_missing_ids_manager import BaseMissingIDsManager
from genie_common.tools import logger


class GeniusMissingIDsManager(BaseMissingIDsManager):
    @property
    def _column(self) -> TrackIDMapping:
        return TrackIDMapping.genius_id

    async def _insert_additional_records(self, matched_ids: Dict[str, Optional[str]]) -> None:
        logger.info("No Genius additional records to insert. Skipping.")
