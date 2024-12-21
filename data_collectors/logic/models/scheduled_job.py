from dataclasses import dataclass
from typing import Callable, Awaitable

from apscheduler.triggers.interval import IntervalTrigger

from data_collectors.jobs.job_id import JobId


@dataclass
class ScheduledJob:
    id: JobId
    task: Callable[[], Awaitable[None]]
    interval: IntervalTrigger
