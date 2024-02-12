from dataclasses import dataclass
from datetime import datetime

from bs4 import BeautifulSoup


@dataclass
class GlglzChartDetails:
    date: datetime
    soup: BeautifulSoup
    datetime_format: str
