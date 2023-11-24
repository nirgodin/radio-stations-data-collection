from dataclasses import dataclass
from typing import Dict


@dataclass
class MusixmatchQuery:
    artist_name: str
    track_name: str

    def to_query_params(self) -> Dict[str, str]:
        return {
            "q_artist": self.artist_name,
            "q_track": self.track_name
        }
