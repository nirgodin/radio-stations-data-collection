from datetime import datetime
from typing import List, Dict, Any, Type

from genie_common.tools import logger, AioPoolExecutor
from genie_common.utils import get_dict_first_key
from genie_datastores.postgres.models import BaseORMModel
from genie_datastores.postgres.utils import update_by_values
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import InstrumentedAttribute

from data_collectors.contract import IDatabaseUpdater
from data_collectors.logic.models import DBUpdateRequest


class ValuesDatabaseUpdater(IDatabaseUpdater):
    def __init__(self, db_engine: AsyncEngine, pool_executor: AioPoolExecutor):
        super().__init__(db_engine)
        self._pool_executor = pool_executor

    async def update(self, update_requests: List[DBUpdateRequest]) -> None:
        n_records = len(update_requests)
        logger.info(f"Starting to update records for {n_records}")
        results = await self._pool_executor.run(  # TODO: Find a way to do it in Bulk
            iterable=update_requests,
            func=self._update_single,
            expected_type=type(None)
        )

        logger.info(f"Successfully updated {len(results)} records out of {n_records}")

    async def _update_single(self, update_request: DBUpdateRequest) -> None:
        orm = self._detect_orm(update_request.values)
        update_request.values[orm.update_date] = datetime.now()

        await update_by_values(
            self._db_engine,
            orm,
            update_request.values,
            orm.id == update_request.id,
        )

    @staticmethod
    def _detect_orm(update_values: Dict[BaseORMModel, Any]) -> Type[BaseORMModel]:
        column: InstrumentedAttribute = get_dict_first_key(update_values)
        return column.class_
