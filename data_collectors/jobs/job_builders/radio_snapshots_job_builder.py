from datetime import datetime

from apscheduler.triggers.interval import IntervalTrigger
from genie_datastores.postgres.models import SpotifyStation
from typing_extensions import Optional

from data_collectors.jobs.base_job_builder import BaseJobBuilder
from data_collectors.jobs.job_id import JobId
from data_collectors.logic.models import ScheduledJob

RADIO_SNAPSHOTS_STATIONS = [
    SpotifyStation.GLGLZ,
    SpotifyStation.KAN_88,
    SpotifyStation.ECO_99,
    SpotifyStation.KAN_GIMEL,
    SpotifyStation.FM_103,
]


class RadioSnapshotsJobBuilder(BaseJobBuilder):
    async def build(self, next_run_time: Optional[datetime] = None) -> ScheduledJob:
        return ScheduledJob(
            task=self._task,
            id=JobId.RADIO_SNAPSHOTS,
            interval=IntervalTrigger(hours=5),
            next_run_time=next_run_time,
        )

    async def _task(self) -> None:
        spotify_session = self._component_factory.sessions.get_spotify_session()

        async with spotify_session as session:
            manager = self._component_factory.misc.get_radio_snapshots_manager(session)
            await manager.run(RADIO_SNAPSHOTS_STATIONS)
