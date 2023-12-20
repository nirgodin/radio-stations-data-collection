from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from genie_datastores.postgres.models import Artist

from data_collectors.logic.models.database_update_request import DBUpdateRequest


@dataclass
class ArtistWikipediaDetails:
    id: str
    birth_date: Optional[datetime]
    death_date: Optional[datetime]

    def to_update_request(self) -> DBUpdateRequest:
        return DBUpdateRequest(
            id=self.id,
            values={
                Artist.birth_date: self.birth_date,
                Artist.death_date: self.death_date
            }
        )
