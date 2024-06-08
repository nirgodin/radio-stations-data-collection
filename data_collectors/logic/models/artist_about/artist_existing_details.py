from datetime import datetime
from typing import Optional

from genie_datastores.postgres.models import Gender
from pydantic import BaseModel
from sqlalchemy.engine import Row


class ArtistExistingDetails(BaseModel):
    id: str
    about: Optional[str]
    origin: Optional[str]
    birth_date: Optional[datetime]
    death_date: Optional[datetime]
    gender: Optional[Gender]

    @classmethod
    def from_row(cls, row: Row) -> "ArtistExistingDetails":
        return cls(
            id=row.id,
            about=row.about,
            origin=row.origin,
            birth_date=row.birth_date,
            death_date=row.death_date,
            gender=row.gender
        )
