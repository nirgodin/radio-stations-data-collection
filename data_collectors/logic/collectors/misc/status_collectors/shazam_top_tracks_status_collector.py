from datetime import timedelta, datetime
from typing import List, Dict

import pandas as pd
from genie_common.utils import sort_dict_by_key
from genie_datastores.postgres.models import ShazamTopTrack, ShazamLocation
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select, func
from sqlalchemy.engine import Row

from data_collectors.consts.general_consts import DAY_LABEL
from data_collectors.contract import IStatusCollector
from data_collectors.logic.models.summary_section import SummarySection


class ShazamTopTracksStatusCollector(IStatusCollector):
    async def collect(self, lookback_period: timedelta) -> List[SummarySection]:
        records = await self._query_records(lookback_period)
        records_by_date = self._group_records_by_date(records)

        return self._to_summary_sections(records_by_date)

    async def _query_records(self, lookback_period: timedelta) -> List[Row]:
        start_date = datetime.now() - lookback_period
        query = (
            select(
                ShazamTopTrack.location,
                func.date_trunc("day", ShazamTopTrack.creation_date).label(DAY_LABEL),
                func.count(),
            )
            .where(ShazamTopTrack.creation_date >= start_date)
            .group_by(ShazamTopTrack.location, DAY_LABEL)
        )
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.all()

    @staticmethod
    def _group_records_by_date(
        records: List[Row],
    ) -> Dict[datetime, List[Dict[str, int]]]:
        grouped_records = {}

        for record in records:
            record_day: datetime = getattr(record, DAY_LABEL)

            if record_day not in grouped_records.keys():
                grouped_records[record_day] = []

            formatted_record = {"Location": record.location.name, "Count": record.count}
            grouped_records[record_day].append(formatted_record)

        return sort_dict_by_key(grouped_records, reverse=False)

    @staticmethod
    def _to_summary_sections(records_by_date: Dict[datetime, List[Dict[str, int]]]) -> List[SummarySection]:
        summaries = []

        for date, records in records_by_date.items():
            section = SummarySection(
                title=date.strftime("%d/%m/%Y"),
                data=pd.DataFrame.from_records(records),
            )
            summaries.append(section)

        return summaries

    @property
    def name(self) -> str:
        return "Shazam Top Tracks"
