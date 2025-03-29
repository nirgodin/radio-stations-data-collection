from data_collectors.logic.inserters.postgres.spotify.tracks_database_inserter import (
    TracksDatabaseInserter,
)
from data_collectors.logic.inserters.postgres.spotify.artists_database_inserter import (
    ArtistsDatabaseInserter,
)
from data_collectors.logic.inserters.postgres.spotify.base_spotify_database_inserter import (
    BaseSpotifyDatabaseInserter,
)
from data_collectors.logic.inserters.postgres.spotify.spotify_albums_database_inserter import (
    SpotifyAlbumsDatabaseInserter,
)
from data_collectors.logic.inserters.postgres.spotify.spotify_artists_database_inserter import (
    SpotifyArtistsDatabaseInserter,
)
from data_collectors.logic.inserters.postgres.spotify.spotify_audio_features_database_inserter import (
    SpotifyAudioFeaturesDatabaseInserter,
)
from data_collectors.logic.inserters.postgres.spotify.spotify_featured_artists_database_artists import (
    SpotifyFeaturedArtistsDatabaseInserter,
)
from data_collectors.logic.inserters.postgres.spotify.spotify_tracks_database_inserter import (
    SpotifyTracksDatabaseInserter,
)
from data_collectors.logic.inserters.postgres.spotify.track_id_mapping_inserter import (
    TrackIDMappingDatabaseInserter,
)

__all__ = [
    "ArtistsDatabaseInserter",
    "BaseSpotifyDatabaseInserter",
    "SpotifyAlbumsDatabaseInserter",
    "SpotifyArtistsDatabaseInserter",
    "SpotifyAudioFeaturesDatabaseInserter",
    "SpotifyFeaturedArtistsDatabaseInserter",
    "SpotifyTracksDatabaseInserter",
    "TrackIDMappingDatabaseInserter",
    "TracksDatabaseInserter",
]
