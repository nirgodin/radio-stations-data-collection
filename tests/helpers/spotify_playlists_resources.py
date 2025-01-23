from dataclasses import dataclass
from typing import Dict


@dataclass
class SpotifyPlaylistsResources:
    id: str
    playlist: dict
    tracks: Dict[str, dict]
    artists: Dict[str, dict]
    albums: Dict[str, dict]

    def __post_init__(self):
        self.track_ids = list(self.tracks.keys())
        self.artist_ids = list(self.artists.keys())
        self.album_ids = list(self.albums.keys())
