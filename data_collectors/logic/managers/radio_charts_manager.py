from typing import Dict, List, Optional

from genie_datastores.google_drive.google_drive_client import GoogleDriveClient
from genie_datastores.postgres.models import Chart, ChartEntry
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors import RadioChartsDataCollector
from data_collectors.consts.radio_charts_consts import EXCLUDED_RADIO_CHARTS_FILES_IDS
from data_collectors.contract import IManager
from data_collectors.logic.collectors.radio_charts.radio_charts_tracks_collector import RadioChartsTracksCollector


class RadioChartsManager(IManager):
    def __init__(self,
                 db_engine: AsyncEngine,
                 drive_client: GoogleDriveClient,
                 charts_data_collector: RadioChartsDataCollector,
                 charts_tracks_collector: RadioChartsTracksCollector):
        self._db_engine = db_engine
        self._drive_client = drive_client
        self._charts_data_collector = charts_data_collector
        self._charts_tracks_collector = charts_tracks_collector

    async def run(self, chart: Chart, limit: Optional[int]) -> None:
        existing_files_names = await self._query_existing_files_names(chart)
        non_existing_files = self._get_non_existing_files(chart, existing_files_names, limit)
        charts_data = await self._charts_data_collector.collect(non_existing_files)

    async def _query_existing_files_names(self, chart: Chart) -> List[str]:
        query = (
            select(ChartEntry.comment)
            .distinct(ChartEntry.comment)
            .where(ChartEntry.chart == chart)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().all()

    def _get_non_existing_files(self, chart: Chart, existing_files_names: List[str], limit: Optional[int]) -> List[dict]:
        drive_dir = self._charts_drive_dir_mapping[chart]
        files = []

        for file in self._drive_client.list_dir_files(drive_dir):
            if len(files) == limit:
                break

            if self._is_relevant_file(file, existing_files_names):
                files.append(file)

        return files

    @property
    def _charts_drive_dir_mapping(self) -> Dict[Chart, str]:
        return {
            Chart.KOL_ISRAEL_WEEKLY_ISRAELI: "1v0otxK72J_1q_PNwUCTvmZTqPXsqMFUO",
            Chart.KOL_ISRAEL_WEEKLY_INTERNATIONAL: "1bSMmwXJrUBNQby5JHRwV-O495l7wgXev",
            Chart.GALATZ_WEEKLY_ISRAELI: "1Om8H2ibRqOpCN-b7CKPUbwhJEHBJ3ZzT"
        }

    @staticmethod
    def _is_relevant_file(file: dict, existing_files_names: List[str]) -> bool:
        if file["name"] not in existing_files_names:
            if file["id"] not in EXCLUDED_RADIO_CHARTS_FILES_IDS:
                return True

        return False
