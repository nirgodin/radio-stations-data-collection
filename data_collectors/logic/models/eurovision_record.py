from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.engine import Row


@dataclass
class EurovisionRecord:
    id: int
    key: str
    date: datetime

    @classmethod
    def from_row(cls, row: Row) -> "EurovisionRecord":
        return cls(id=row.id, key=row.key, date=row.date)
