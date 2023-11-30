from typing import Dict, Optional

from genie_datastores.postgres.models import TrackIDMapping
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.logic.collectors.shazam import ShazamSearchCollector
from data_collectors.logic.inserters import ShazamInsertionsManager
from data_collectors.logic.managers.missing_ids_managers.base_missing_ids_manager import BaseMissingIDsManager
from data_collectors.logic.updaters import TrackIDsDatabaseUpdater


class ShazamMissingIDsManager(BaseMissingIDsManager):
    def __init__(self,
                 db_engine: AsyncEngine,
                 search_collector: ShazamSearchCollector,
                 track_ids_updater: TrackIDsDatabaseUpdater,
                 insertions_manager: ShazamInsertionsManager):
        super().__init__(db_engine=db_engine, search_collector=search_collector, track_ids_updater=track_ids_updater)
        self._insertions_manager = insertions_manager

    @property
    def _column(self) -> TrackIDMapping:
        return TrackIDMapping.shazam_id

    async def _insert_additional_records(self, matched_ids: Dict[str, Optional[str]]) -> None:
        non_missing_shazam_ids = [shazam_id for shazam_id in matched_ids.values() if shazam_id is not None]
        await self._insertions_manager.insert(non_missing_shazam_ids)
