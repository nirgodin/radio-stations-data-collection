from typing import Dict, List, Optional, Any

from genie_common.tools import logger
from genie_datastores.google.drive import GoogleDriveClient
from genie_datastores.postgres.models import Chart, ChartEntry
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.radio_charts_consts import EXCLUDED_RADIO_CHARTS_FILES_IDS
from data_collectors.logic.collectors import RadioChartsDataCollector, RadioChartsTracksCollector
from data_collectors.logic.inserters.postgres import SpotifyInsertionsManager, ChartEntriesDatabaseInserter
from data_collectors.logic.managers.radio_charts.base_radio_charts_manager import BaseRadioChartsManager


class RadioChartsManager(BaseRadioChartsManager):
    def __init__(self,
                 charts_data_collector: RadioChartsDataCollector,
                 charts_tracks_collector: RadioChartsTracksCollector,
                 spotify_insertions_manager: SpotifyInsertionsManager,
                 chart_entries_inserter: ChartEntriesDatabaseInserter,
                 db_engine: AsyncEngine,
                 drive_client: GoogleDriveClient):
        super().__init__(charts_data_collector, charts_tracks_collector, spotify_insertions_manager, chart_entries_inserter)
        self._drive_client = drive_client
        self._db_engine = db_engine

    async def _generate_data_collector_order_args(self, chart: Chart, limit: Optional[int]) -> Dict[str, Any]:
        existing_files_names = await self._query_existing_files_names(chart)
        logger.info("Starting to select non existing charts to insert")
        drive_dir = self._charts_drive_dir_mapping[chart]
        files = []

        for file in self._drive_client.list_dir_files(drive_dir):
            if len(files) == limit:
                break

            if self._is_relevant_file(file, existing_files_names):
                files.append(file)

        return {
            "chart_drive_files": files,
            "chart": chart
        }

    async def _query_existing_files_names(self, chart: Chart) -> List[str]:
        logger.info(f"Querying existing files names to prevent double insertion of same chart entries")
        query = (
            select(ChartEntry.comment)
            .distinct(ChartEntry.comment)
            .where(ChartEntry.chart == chart)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().all()

    @staticmethod
    def _is_relevant_file(file: dict, existing_files_names: List[str]) -> bool:
        if file["name"] not in existing_files_names:
            if file["id"] not in EXCLUDED_RADIO_CHARTS_FILES_IDS:
                return True

        return False

    @property
    def _charts_drive_dir_mapping(self) -> Dict[Chart, str]:
        return {
            Chart.KOL_ISRAEL_WEEKLY_ISRAELI: "1v0otxK72J_1q_PNwUCTvmZTqPXsqMFUO",
            Chart.KOL_ISRAEL_WEEKLY_INTERNATIONAL: "1bSMmwXJrUBNQby5JHRwV-O495l7wgXev",
            Chart.GALATZ_WEEKLY_ISRAELI: "1Om8H2ibRqOpCN-b7CKPUbwhJEHBJ3ZzT"
        }
