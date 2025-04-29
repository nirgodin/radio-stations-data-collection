from datetime import datetime
from typing import List

from genie_common.utils import chain_lists
from genie_datastores.postgres.models import ChartEntry
from pydantic import BaseModel, validator

from data_collectors.logic.models.raw_entry import RawEntry


class DailyChart(BaseModel):
    date: datetime
    entries: List[RawEntry]

    @validator("date")
    def _normalize_date(cls, date: datetime) -> datetime:
        return date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    def to_charts_entries(self, url: str) -> List[ChartEntry]:
        entries: List[List[ChartEntry]] = [entry.to_charts_entries(self.date, url) for entry in self.entries]
        return chain_lists(entries)
