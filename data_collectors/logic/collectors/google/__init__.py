from data_collectors.logic.collectors.google.artists_about.base_artist_existing_details_collector import (
    BaseArtistsExistingDetailsCollector,
)
from data_collectors.logic.collectors.google.artists_about.existing_details_collectors.genius_artist_existing_details_collector import (
    GeniusArtistsExistingDetailsCollector,
)
from data_collectors.logic.collectors.google.artists_about.existing_details_collectors.shazam_artist_existing_details_collector import (
    ShazamArtistsExistingDetailsCollector,
)
from data_collectors.logic.collectors.google.artists_about.existing_details_collectors.spotify_artist_existing_details_collector import (
    SpotifyArtistsExistingDetailsCollector,
)
from data_collectors.logic.collectors.google.artists_about.existing_details_collectors.wikipedia_artist_existing_details_collector import (
    WikipediaArtistsExistingDetailsCollector,
)
from data_collectors.logic.collectors.google.artists_about.gemini_artists_about_parsing_collector import (
    GeminiArtistsAboutParsingCollector,
)
from data_collectors.logic.collectors.google.google_artists_web_pages_collector import GoogleArtistsWebPagesCollector
from data_collectors.logic.collectors.google.google_geocoding_collector import (
    GoogleGeocodingCollector,
)

__all__ = [
    "BaseArtistsExistingDetailsCollector",
    "GeniusArtistsExistingDetailsCollector",
    "ShazamArtistsExistingDetailsCollector",
    "WikipediaArtistsExistingDetailsCollector",
    "SpotifyArtistsExistingDetailsCollector",
    "GeminiArtistsAboutParsingCollector",
    "GoogleArtistsWebPagesCollector",
    "GoogleGeocodingCollector",
]
