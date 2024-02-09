from typing import Dict, List

from genie_common.tools import logger
from genie_datastores.postgres.models import Chart, ChartEntry, Decision, DataSource, Table, Artist, \
    SpotifyArtist, SpotifyTrack
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine

from data_collectors.contract import IManager
from data_collectors.logic.inserters.postgres import ChunksDatabaseInserter
from data_collectors.logic.models import DBUpdateRequest
from data_collectors.logic.updaters import ValuesDatabaseUpdater


class ChartsIsraeliArtistsManager(IManager):
    def __init__(self, db_engine: AsyncEngine, db_updater: ValuesDatabaseUpdater, db_inserter: ChunksDatabaseInserter):
        self._db_engine = db_engine
        self._db_updater = db_updater
        self._db_inserter = db_inserter

    async def run(self, charts_mapping: Dict[Chart, bool]) -> None:
        logger.info("Starting to update tracks `is_israeli` field using charts mapping")

        for chart, is_israeli in charts_mapping.items():
            await self._update_single_chart_artists(chart, is_israeli)
            logger.info(f"Successfully updated chart `{chart.value}` artists")

    async def _update_single_chart_artists(self, chart: Chart, is_israeli: bool) -> None:
        artists_ids = await self._query_unique_chart_artists_ids(chart, is_israeli)

        if not artists_ids:
            logger.info(f"Did not find any relevant artist for chart `{chart.value}`. Skipping")
            return

        update_requests = self._to_update_requests(chart, artists_ids, is_israeli)
        await self._db_updater.update(update_requests)
        decision_records = self._to_decision_records(chart, artists_ids)
        await self._db_inserter.insert(decision_records)

    async def _query_unique_chart_artists_ids(self, chart: Chart, is_israeli: bool) -> List[str]:
        logger.info(f"Querying chart `{chart.value}` unique tracks ids")
        query = (
            select(Artist.id)
            .distinct(Artist.id)
            .where(ChartEntry.track_id == SpotifyTrack.id)
            .where(SpotifyTrack.artist_id == SpotifyArtist.id)
            .where(SpotifyArtist.id == Artist.id)
            .where(ChartEntry.chart == chart)
            .where(ChartEntry.track_id.isnot(None))
            .where(Artist.is_israeli.isnot(is_israeli))
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.scalars().all()

    @staticmethod
    def _to_update_requests(chart: Chart, artists_ids: List[str], is_israeli: bool) -> List[DBUpdateRequest]:
        logger.info(f"Transforming chart `{chart.value}` to update requests")
        update_requests = []

        for artist_id in artists_ids:
            request = DBUpdateRequest(
                id=artist_id,
                values={Artist.is_israeli: is_israeli}
            )
            update_requests.append(request)

        return update_requests

    @staticmethod
    def _to_decision_records(chart: Chart, artists_ids: List[str]) -> List[Decision]:
        logger.info(f"Transforming chart `{chart.value}` to decision entries")
        records = []

        for artist_id in artists_ids:
            decision = Decision(
                column=Artist.is_israeli.key,
                source=DataSource.CHARTS,
                table=Table.ARTISTS,
                table_id=artist_id
            )
            records.append(decision)

        return records
