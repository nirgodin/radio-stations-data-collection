from dataclasses import dataclass
from typing import Optional

from genie_datastores.postgres.models import ChartEntry


@dataclass
class RadioChartEntryDetails:
    entry: ChartEntry
    track: Optional[dict] = None
