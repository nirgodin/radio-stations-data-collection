from datetime import datetime
from typing import List

from genie_common.utils import chain_lists
from genie_datastores.postgres.models import ChartEntry
from pydantic import BaseModel, validator

from data_collectors.logic.models.glglz.raw_glglz_entry import RawGlglzEntry


class GlglzChartDetails(BaseModel):
    date: datetime
    entries: List[RawGlglzEntry]

    def to_charts_entries(self, url: str) -> List[ChartEntry]:
        entries: List[List[ChartEntry]] = [entry.to_charts_entries(self.date, url) for entry in self.entries]
        return chain_lists(entries)

    @validator("date")
    def _normalize_date(cls, date: datetime) -> datetime:
        return date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
