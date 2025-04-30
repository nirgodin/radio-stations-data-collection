from datetime import datetime
from typing import List

from genie_datastores.postgres.models import ChartEntry
from pydantic import BaseModel

from data_collectors.logic.models.glglz.chart_origin import ChartOrigin
from data_collectors.logic.models.glglz.localized_entity import LocalizedEntity


class RawGlglzEntry(BaseModel):
    raw_value: str
    artist: LocalizedEntity
    track: LocalizedEntity
    position: int
    origin: ChartOrigin

    def to_charts_entries(self, date: datetime, url: str) -> List[ChartEntry]:
        charts_entries = []

        for key in self._build_key_permutations():
            chart_entry = self._to_chart_entry(date=date, url=url, key=key)
            charts_entries.append(chart_entry)

        return charts_entries

    def _build_key_permutations(self) -> List[str]:
        permutations = [f"{self.artist.name} - {self.track.name}"]
        if self.artist.translation is not None:
            permutations.append(f"{self.artist.translation} - {self.track.name}")

        if self.track.translation is not None:
            permutations.append(f"{self.artist.name} - {self.track.translation}")

        if self.artist.translation is not None and self.track.translation is not None:
            permutations.append(f"{self.artist.translation} - {self.track.translation}")

        return permutations

    def _to_chart_entry(self, date: datetime, url: str, key: str):
        return ChartEntry(
            chart=self.origin.to_glglz_chart(),
            date=date,
            position=self.position,
            comment=url,
            key=key,
        )
