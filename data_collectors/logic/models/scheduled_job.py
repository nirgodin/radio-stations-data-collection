from dataclasses import dataclass
from typing import Callable, Awaitable

from apscheduler.triggers.interval import IntervalTrigger


@dataclass
class ScheduledJob:
    id: str
    task: Callable[[], Awaitable[None]]
    interval: IntervalTrigger
