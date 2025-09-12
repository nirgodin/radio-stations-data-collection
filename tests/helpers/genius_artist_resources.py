from dataclasses import dataclass
from typing import Dict, Any

from genie_common.utils import random_alphanumeric_string
from genie_datastores.postgres.models import Artist, SpotifyArtist
from spotipyio.testing import SpotifyMockFactory


@dataclass
class GeniusArtistResources:
    genius_id: str
    spotify_id: str
    name: str

    @classmethod
    def random(cls) -> "GeniusArtistResources":
        return cls(
            genius_id=random_alphanumeric_string(),
            spotify_id=SpotifyMockFactory.spotify_id(),
            name=SpotifyMockFactory.name(),
        )

    def to_artist(self) -> Artist:
        return Artist(id=self.spotify_id)

    def to_spotify_artist(self) -> SpotifyArtist:
        return SpotifyArtist(id=self.spotify_id, name=self.name)

    def to_search_response(self) -> Dict[str, Any]:
        return {
            "response": {
                "sections": [
                    {
                        "type": "artist",
                        "hits": [
                            {
                                "result": {
                                    "id": self.genius_id,
                                    "name": self.name,
                                }
                            }
                        ],
                    }
                ]
            }
        }
