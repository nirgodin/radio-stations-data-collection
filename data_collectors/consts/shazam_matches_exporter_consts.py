from genie_datastores.postgres.models import (
    SpotifyTrack,
    SpotifyArtist,
    ShazamTrack,
    ShazamArtist,
)

SPOTIFY_TRACK_NAME_COLUMN = "spotify_track_name"
SPOTIFY_ARTIST_NAME_COLUMN = "spotify_artist_name"
SHAZAM_TRACK_NAME_COLUMN = "shazam_track_name"
SHAZAM_ARTIST_NAME_COLUMN = "shazam_artist_name"
QUERY_COLUMNS = [
    SpotifyTrack.name.label(SPOTIFY_TRACK_NAME_COLUMN),
    SpotifyArtist.name.label(SPOTIFY_ARTIST_NAME_COLUMN),
    ShazamTrack.name.label(SHAZAM_TRACK_NAME_COLUMN),
    ShazamArtist.name.label(SHAZAM_ARTIST_NAME_COLUMN),
    SpotifyTrack.id,
    ShazamTrack.id.label("shazam_id"),
]
SPOTIFY_KEY_COLUMN = "spotify_key"
SHAZAM_KEY_COLUMN = "shazam_key"
