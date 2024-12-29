from typing import Optional, List, Dict

from genie_common.tools import logger
from genie_common.utils import safe_nested_get
from genie_datastores.postgres.models import ChartEntry, Chart
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.consts.spotify_consts import ID, TRACK
from data_collectors.contract import IManager
from data_collectors.logic.collectors import EurovisionMissingTracksCollector
from data_collectors.logic.inserters.postgres import SpotifyInsertionsManager
from data_collectors.logic.models import DBUpdateRequest, EurovisionRecord
from data_collectors.logic.updaters import ValuesDatabaseUpdater


class EurovisionMissingTracksManager(IManager):
    def __init__(
        self,
        db_engine: AsyncEngine,
        missing_tracks_collector: EurovisionMissingTracksCollector,
        spotify_insertions_manager: SpotifyInsertionsManager,
        db_updater: ValuesDatabaseUpdater,
    ):
        self._db_engine = db_engine
        self._missing_tracks_collector = missing_tracks_collector
        self._spotify_insertions_manager = spotify_insertions_manager
        self._db_updater = db_updater

    async def run(self, limit: Optional[int]) -> None:
        logger.info("Starting to run EurovisionMissingTracksManager")
        tracks_keys_and_dates = await self._query_missing_eurovision_tracks(limit)
        chart_id_tracks_mapping = await self._missing_tracks_collector.collect(
            tracks_keys_and_dates
        )
        tracks = list(chart_id_tracks_mapping.values())
        logger.info("Inserting collected tracks to spotify tables")
        await self._spotify_insertions_manager.insert(tracks)
        await self._update_charts_entries_records(chart_id_tracks_mapping)

    async def _query_missing_eurovision_tracks(
        self, limit: Optional[int]
    ) -> List[EurovisionRecord]:
        logger.info("Querying database for Eurovision records without track id")
        query = (
            select(ChartEntry.id, ChartEntry.key, ChartEntry.date)
            .where(ChartEntry.chart == Chart.EUROVISION)
            .where(ChartEntry.track_id.is_(None))
            .order_by(ChartEntry.date.desc())
            .limit(limit)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return [EurovisionRecord.from_row(row) for row in query_result.all()]

    async def _update_charts_entries_records(
        self, chart_id_tracks_mapping: Dict[int, dict]
    ) -> None:
        logger.info("Updating charts_entries table records with collected tracks ids")
        update_requests = self._to_update_requests(chart_id_tracks_mapping)

        await self._db_updater.update(update_requests)

    @staticmethod
    def _to_update_requests(
        chart_id_tracks_mapping: Dict[int, dict]
    ) -> List[DBUpdateRequest]:
        update_requests = []

        for id_, track in chart_id_tracks_mapping.items():
            track_id = safe_nested_get(track, [TRACK, ID])
            request = DBUpdateRequest(id=id_, values={ChartEntry.track_id: track_id})
            update_requests.append(request)

        return update_requests
