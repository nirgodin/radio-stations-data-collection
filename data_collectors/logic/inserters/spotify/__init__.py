from data_collectors.logic.inserters.spotify.base_spotify_database_inserter import \
    BaseSpotifyDatabaseInserter
from data_collectors.logic.inserters.spotify.spotify_albums_database_inserter import \
    SpotifyAlbumsDatabaseInserter
from data_collectors.logic.inserters.spotify.spotify_artists_database_inserter import \
    SpotifyArtistsDatabaseInserter
from data_collectors.logic.inserters.spotify.spotify_audio_features_database_inserter import \
    SpotifyAudioFeaturesDatabaseInserter
from data_collectors.logic.inserters.spotify.spotify_tracks_database_inserter import \
    SpotifyTracksDatabaseInserter
from data_collectors.logic.inserters.spotify.track_id_mapping_inserter import \
    TrackIDMappingDatabaseInserter


__all__ = [
    "SpotifyAlbumsDatabaseInserter",
    "SpotifyArtistsDatabaseInserter",
    "SpotifyAudioFeaturesDatabaseInserter",
    "SpotifyTracksDatabaseInserter",
    "TrackIDMappingDatabaseInserter",
    "BaseSpotifyDatabaseInserter"
]
