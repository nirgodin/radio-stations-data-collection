from __future__ import annotations

from dataclasses import dataclass

from spotipyio.testing import SpotifyMockFactory


@dataclass
class SpotifyTrackResources:
    album_id: str
    artist_id: str
    artist_name: str
    track_id: str
    track_name: str

    def __post_init__(self) -> None:
        self.artist = SpotifyMockFactory.artist(
            id=self.artist_id, name=self.artist_name
        )
        self.album = SpotifyMockFactory.album(id=self.album_id, artists=[self.artist])
        self.track = SpotifyMockFactory.track(
            id=self.track_id,
            name=self.track_name,
            artists=[self.artist],
            album=self.album,
        )

    @classmethod
    def random(cls) -> SpotifyTrackResources:
        return cls(
            album_id=SpotifyMockFactory.spotify_id(),
            artist_id=SpotifyMockFactory.spotify_id(),
            artist_name=SpotifyMockFactory.name(),
            track_id=SpotifyMockFactory.spotify_id(),
            track_name=SpotifyMockFactory.name(),
        )
