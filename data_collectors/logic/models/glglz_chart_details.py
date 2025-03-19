from dataclasses import dataclass
from datetime import datetime


@dataclass
class GlglzChartDetails:
    date: datetime
    html: str
    datetime_format: str
