from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class ArtistWikipediaDetails:
    id: str
    birth_date: Optional[datetime]
    death_date: Optional[datetime]
