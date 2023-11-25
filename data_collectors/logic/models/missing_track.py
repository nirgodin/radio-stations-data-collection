from dataclasses import dataclass

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
