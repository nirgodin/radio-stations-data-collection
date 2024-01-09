from dataclasses import dataclass
from datetime import datetime

from pandas import DataFrame


@dataclass
class RadioChartData:
    data: DataFrame
    date: datetime
    original_file_name: str
