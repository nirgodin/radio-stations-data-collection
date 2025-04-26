from datetime import timedelta, datetime
from typing import List, Dict

import pandas as pd
from genie_datastores.postgres.models import (
    RadioTrack,
    SpotifyStation,
    SpotifyTrack,
    SpotifyArtist,
)
from genie_datastores.postgres.operations import execute_query
from sqlalchemy import select, func
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql import Subquery

from data_collectors.contract import IStatusCollector
from data_collectors.logic.models.summary_section import SummarySection

ARTIST_NAME_LABEL = "artist_name"
PLAY_COUNT_LABEL = "play_count"
STATION_RANK_LABEL = "station_rank"
EXCLUDED_TRACKS_IDS = [
    "6LO68LyIClSBW3uip2ppOF",
    "2Er6raJ45LlC64yhzFqKmg",
    "2xhNmWROsvJimCDsPPvr0B",
    "3vwkVN4D6PWAvva13dfimS",
]


class RadioTracksTopTracksStatusCollector(IStatusCollector):
    def __init__(self, db_engine: AsyncEngine, limit: int = 10):
        super().__init__(db_engine)
        self._limit = limit

    async def collect(self, lookback_period: timedelta) -> List[SummarySection]:
        records = await self._query_records(lookback_period)
        records_by_station = self._group_records_to_station_sections(records)

        return self._to_summary_sections(records_by_station)

    async def _query_records(self, lookback_period: timedelta) -> List[Row]:
        start_date = datetime.now() - lookback_period
        subquery = self._build_most_played_tracks_by_station_subquery(start_date)
        columns = [
            subquery.c.station,
            subquery.c.name,
            getattr(subquery.c, ARTIST_NAME_LABEL),
            getattr(subquery.c, PLAY_COUNT_LABEL),
        ]
        query = select(*columns).where(getattr(subquery.c, STATION_RANK_LABEL) <= self._limit)
        query_result = await execute_query(engine=self._db_engine, query=query)

        return query_result.all()

    @staticmethod
    def _build_most_played_tracks_by_station_subquery(start_date: datetime) -> Subquery:
        track_play_count_rank = (
            func.row_number()
            .over(partition_by=[RadioTrack.station], order_by=func.count().desc())
            .label(STATION_RANK_LABEL)
        )
        columns = [
            RadioTrack.station,
            SpotifyTrack.name,
            SpotifyArtist.name.label(ARTIST_NAME_LABEL),
            func.count().label(PLAY_COUNT_LABEL),
            track_play_count_rank,
        ]
        query = (
            select(*columns)
            .where(RadioTrack.track_id == SpotifyTrack.id)
            .where(SpotifyTrack.artist_id == SpotifyArtist.id)
            .where(RadioTrack.creation_date >= start_date)
            .where(SpotifyTrack.id.notin_(EXCLUDED_TRACKS_IDS))
            .group_by(RadioTrack.station, SpotifyTrack.name, SpotifyArtist.name)
        )

        return query.subquery()

    @staticmethod
    def _group_records_to_station_sections(records: List[Row]) -> Dict[SpotifyStation, List[dict]]:
        grouped_records: Dict[SpotifyStation, List[dict]] = {}

        for record in records:
            if record.station not in grouped_records.keys():
                grouped_records[record.station] = []

            item = {"Artist": record.artist_name, "Song": record.name, "Play Count": record.play_count}
            grouped_records[record.station].append(item)

        return grouped_records

    @staticmethod
    def _to_summary_sections(grouped_records: Dict[SpotifyStation, List[dict]]) -> List[SummarySection]:
        summary_sections = []

        for station, records in grouped_records.items():
            data = pd.DataFrame.from_records(records)
            section = SummarySection(title=station.name, data=data)
            summary_sections.append(section)

        return summary_sections

    @property
    def name(self) -> str:
        return "Top Tracks by Station"
