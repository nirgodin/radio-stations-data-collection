from typing import List

from postgres_client import ChartEntryData, BillboardTrack

from data_collectors.contract import BaseDatabaseUpdater


class BillboardTracksDatabaseUpdater(BaseDatabaseUpdater):
    async def update(self, charts_entries: List[ChartEntryData]) -> None:
        ids_weeks_mapping = {entry.id: entry.entry.weeks for entry in charts_entries}
        await self._update_by_mapping(
            mapping=ids_weeks_mapping,
            orm=BillboardTrack,
            key_column=BillboardTrack.id,
            value_column=BillboardTrack.weeks_on_chart
        )
