from dataclasses import dataclass
from typing import Optional


@dataclass
class SpotifyArtistAbout:
    id: str
    wikipedia_name: Optional[str] = None
    wikipedia_language: Optional[str] = None
    facebook_name: Optional[str] = None
    instagram_name: Optional[str] = None
    twitter_name: Optional[str] = None
    about: Optional[str] = None
