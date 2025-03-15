from __future__ import annotations

from dataclasses import dataclass

from spotipyio.testing import SpotifyMockFactory


@dataclass
class SpotifyTrackResources:
    artist_id: str
    artist_name: str
    track_id: str
    track_name: str

    def __post_init__(self) -> None:
        self.artist = SpotifyMockFactory.artist(
            id=self.artist_id, name=self.artist_name
        )
        self.track = SpotifyMockFactory.track(
            id=self.track_id, name=self.track_name, artists=[self.artist]
        )
        self.album = SpotifyMockFactory.album(artists=[self.artist])

    @classmethod
    def random(cls) -> SpotifyTrackResources:
        return cls(
            artist_id=SpotifyMockFactory.spotify_id(),
            artist_name=SpotifyMockFactory.name(),
            track_id=SpotifyMockFactory.spotify_id(),
            track_name=SpotifyMockFactory.name(),
        )
