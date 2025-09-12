from dataclasses import dataclass
from typing import Optional

from sqlalchemy.engine import Row


@dataclass
class GeocodingResponse:
    id: str
    result: Optional[dict]
    is_from_cache: bool

    @classmethod
    def from_row(cls, id: str, row: Row) -> "GeocodingResponse":
        result = {}

        return cls(id=row.id, result=result, is_from_cache=True)
