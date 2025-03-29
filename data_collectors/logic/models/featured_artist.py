from pydantic import BaseModel

from data_collectors.consts.spotify_consts import ID


class FeaturedArtist(BaseModel):
    id: str
    track_id: str
    position: int

    @classmethod
    def from_raw_artist(cls, track_id: str, index: int, artist: dict) -> "FeaturedArtist":
        return cls(
            id=artist[ID],
            track_id=track_id,
            position=index + 1,
        )
