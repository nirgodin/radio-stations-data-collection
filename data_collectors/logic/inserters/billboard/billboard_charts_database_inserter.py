from typing import List

from genie_datastores.postgres.models import BillboardChartEntry, ChartEntryData
from genie_datastores.postgres.operations import insert_records

from data_collectors.contract.base_database_inserter import BaseDatabaseInserter
from data_collectors.logs import logger


class BillboardChartsDatabaseInserter(BaseDatabaseInserter):
    async def insert(self, charts_entries: List[ChartEntryData]) -> List[BillboardChartEntry]:
        logger.info(f"Starting to insert {len(charts_entries)} Billboard chart entries")
        records = [BillboardChartEntry.from_chart_entry(entry) for entry in charts_entries]
        await insert_records(engine=self._db_engine, records=records)
        logger.info(f"Successfully inserted Billboard chart entries")

        return records
