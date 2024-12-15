from dataclasses import dataclass

from spotipyio.models import MatchingEntity
from sqlalchemy.engine import Row


@dataclass(frozen=True)
class MissingTrack:
    spotify_id: str
    artist_name: str
    track_name: str

    @classmethod
    def from_row(cls, row: Row) -> "MissingTrack":
        return cls(
            spotify_id=row.id,
            artist_name=row.artist_name,
            track_name=row.name
        )

    def to_matching_entity(self) -> MatchingEntity:
        return MatchingEntity(
            track=self.track_name,
            artist=self.artist_name
        )
