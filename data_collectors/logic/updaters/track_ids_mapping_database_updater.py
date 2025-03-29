from typing import Optional, Dict

from genie_common.tools import logger
from genie_datastores.postgres.models import TrackIDMapping
from genie_datastores.postgres.models.orm.base_orm_model import BaseORMModel
from genie_datastores.postgres.utils import update_by_mapping

from data_collectors.contract import IDatabaseUpdater


class TrackIDsMappingDatabaseUpdater(IDatabaseUpdater):
    async def update(self, ids_mapping: Dict[str, Optional[str]], value_column: BaseORMModel) -> None:
        logger.info(f"Starting to update ids in tracks_ids_mapping table for {len(ids_mapping)} records")
        await update_by_mapping(
            engine=self._db_engine,
            mapping=ids_mapping,
            orm=TrackIDMapping,
            key_column=TrackIDMapping.id,
            value_column=value_column,
        )
        logger.info("Successfully update ids mapping records with shazam ids")
