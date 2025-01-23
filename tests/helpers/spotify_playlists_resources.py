from dataclasses import dataclass
from typing import List


@dataclass
class SpotifyPlaylistsResources:
    id: str
    playlist: dict
    tracks: List[str]
    artists: List[str]
    albums: List[str]
