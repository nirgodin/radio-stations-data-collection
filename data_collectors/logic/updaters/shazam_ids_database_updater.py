from typing import Optional, Dict

from postgres_client import TrackIDMapping

from data_collectors.contract import BaseDatabaseUpdater
from data_collectors.logs import logger


class ShazamIDsDatabaseUpdater(BaseDatabaseUpdater):
    async def update(self, ids_mapping: Dict[str, Optional[str]]) -> None:
        logger.info(f"Starting to update shazam ids in tracks_ids_mapping table for {len(ids_mapping)} records")
        await self._update_by_mapping(
            mapping=ids_mapping,
            orm=TrackIDMapping,
            key_column=TrackIDMapping.id,
            value_column=TrackIDMapping.shazam_id
        )
        logger.info(f"Successfully update ids mapping records with shazam ids")
