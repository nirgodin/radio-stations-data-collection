from dataclasses import dataclass
from datetime import datetime


@dataclass
class DateRange:
    start_date: datetime
    end_date: datetime
