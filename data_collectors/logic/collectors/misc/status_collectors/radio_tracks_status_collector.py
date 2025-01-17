from datetime import timedelta, datetime
from typing import List, Dict

from genie_common.utils import sort_dict_by_key
from genie_datastores.postgres.models import RadioTrack, SpotifyStation
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select, func
from sqlalchemy.engine import Row

from data_collectors.consts.general_consts import DAY_LABEL
from data_collectors.contract import IStatusCollector


class RadioTracksStatusCollector(IStatusCollector):
    async def collect(self, lookback_period: timedelta) -> str:
        records = await self._query_records(lookback_period)
        records_by_date = self._group_records_by_date(records)

        return self._summarize(records_by_date)

    async def _query_records(self, lookback_period: timedelta) -> List[Row]:
        start_date = datetime.now() - lookback_period
        query = (
            select(
                RadioTrack.station,
                func.date_trunc("day", RadioTrack.creation_date).label(DAY_LABEL),
                func.count(),
            )
            .where(RadioTrack.creation_date >= start_date)
            .group_by(RadioTrack.station, DAY_LABEL)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.all()

    @staticmethod
    def _group_records_by_date(
        records: List[Row],
    ) -> Dict[datetime, Dict[SpotifyStation, int]]:
        grouped_records = {}

        for record in records:
            record_day: datetime = getattr(record, DAY_LABEL)

            if record_day not in grouped_records.keys():
                grouped_records[record_day] = {}

            grouped_records[record_day][record.station] = record.count

        return sort_dict_by_key(grouped_records, reverse=False)

    @staticmethod
    def _summarize(records_by_date: Dict[datetime, Dict[SpotifyStation, int]]) -> str:
        summaries = []

        for date, records in records_by_date.items():
            date_summary = f"{date.strftime('%d/%m/%Y')}:"

            for station, count in records.items():
                date_summary += f"\n{station.name}: {count}"

            summaries.append(date_summary)

        return "\n\n".join(summaries)

    @property
    def name(self) -> str:
        return "Radio Tracks"
