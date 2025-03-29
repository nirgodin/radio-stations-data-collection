from typing import List

from genie_common.tools import logger, AioPoolExecutor
from genie_common.utils import chain_lists, string_to_boolean
from genie_datastores.postgres.models import ChartEntry, Chart
from genie_datastores.postgres.operations import execute_query
from pandas import Series, DataFrame
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import ICollector
from data_collectors.logic.models import DBUpdateRequest


class ChartsTaggedMistakesCollector(ICollector):
    def __init__(self, pool_executor: AioPoolExecutor, db_engine: AsyncEngine):
        self._pool_executor = pool_executor
        self._db_engine = db_engine

    async def collect(self, data: DataFrame) -> List[DBUpdateRequest]:
        logger.info("Collecting update requests from mistakes data")
        self._pre_process_data(data)
        non_handled_data = data[data["done"] == False]
        results = await self._pool_executor.run(
            iterable=[row for _, row in non_handled_data.iterrows()],
            func=self._to_update_requests,
            expected_type=list,
        )

        return chain_lists(results)

    async def _to_update_requests(self, row: Series) -> List[DBUpdateRequest]:
        existing_rows_ids = await self._query_existing_rows_ids(row)
        update_requests = []

        for id_ in existing_rows_ids:
            request = DBUpdateRequest(id=id_, values={ChartEntry.track_id: row["correct_track_id"]})
            update_requests.append(request)

        return update_requests

    async def _query_existing_rows_ids(self, row: Series) -> List[int]:
        query = select(ChartEntry.id).where(ChartEntry.chart == Chart(row["chart"])).where(ChartEntry.key == row["key"])
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().all()

    @staticmethod
    def _pre_process_data(data: DataFrame) -> None:
        data["chart"] = data["chart"].str.lower()
        data["done"] = data["done"].apply(string_to_boolean)
        data["correct_track_id"] = data["correct_track_id"].replace("", None)
