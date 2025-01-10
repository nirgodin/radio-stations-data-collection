from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Awaitable, Optional

from apscheduler.triggers.interval import IntervalTrigger

from data_collectors.jobs.job_id import JobId


@dataclass
class ScheduledJob:
    id: JobId
    task: Callable[[], Awaitable[None]]
    interval: IntervalTrigger
    next_run_time: Optional[datetime] = None
