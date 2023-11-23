from dataclasses import dataclass

from sqlalchemy.engine import Row


@dataclass
class MissingTrack:
    spotify_id: str
    artist_name: str
    track_name: str

    def __post_init__(self):
        self.query = f"{self.artist_name} - {self.track_name}"

    @classmethod
    def from_row(cls, row: Row) -> "MissingTrack":
        return cls(
            spotify_id=row.id,
            artist_name=row.artist_name,
            track_name=row.name
        )
