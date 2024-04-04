from typing import List

from genie_datastores.postgres.models import BillboardChartEntry, ChartEntryData
from genie_datastores.postgres.operations import insert_records

from data_collectors.contract.inserters.postgres_database_inserter_interface import IPostgresDatabaseInserter
from genie_common.tools import logger

from data_collectors.logic.inserters.postgres import ChunksDatabaseInserter


class BillboardChartsDatabaseInserter:
    def __init__(self, chunks_inserter: ChunksDatabaseInserter):
        self._chunks_inserter = chunks_inserter

    async def insert(self, charts_entries: List[ChartEntryData]) -> List[BillboardChartEntry]:
        logger.info(f"Starting to insert {len(charts_entries)} Billboard chart entries")
        records = [BillboardChartEntry.from_chart_entry(entry) for entry in charts_entries]
        await self._chunks_inserter.insert(records)
        logger.info(f"Successfully inserted Billboard chart entries")

        return records
