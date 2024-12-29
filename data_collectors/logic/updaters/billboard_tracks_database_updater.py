from typing import List

from genie_datastores.postgres.models import ChartEntryData, BillboardTrack
from genie_datastores.postgres.utils import update_by_mapping

from data_collectors.contract import IDatabaseUpdater


class BillboardTracksDatabaseUpdater(IDatabaseUpdater):
    async def update(self, charts_entries: List[ChartEntryData]) -> None:
        ids_weeks_mapping = {entry.id: entry.entry.weeks for entry in charts_entries}
        await update_by_mapping(
            engine=self._db_engine,
            mapping=ids_weeks_mapping,
            orm=BillboardTrack,
            key_column=BillboardTrack.id,
            value_column=BillboardTrack.weeks_on_chart,
        )
