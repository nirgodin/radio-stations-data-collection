from data_collectors.logic.inserters.postgres.billboard import *
from data_collectors.logic.inserters.postgres.shazam import *
from data_collectors.logic.inserters.postgres.spotify import *
from data_collectors.logic.inserters.postgres.spotify_insertions_manager import SpotifyInsertionsManager
from data_collectors.logic.inserters.postgres.shazam_insertions_manager import ShazamInsertionsManager
from data_collectors.logic.inserters.postgres.radio_tracks_database_inserter import RadioTracksDatabaseInserter

__all__ = [
    # Billboard
    "BillboardChartsDatabaseInserter",
    "BillboardTracksDatabaseInserter",

    # Spotify
    "SpotifyAlbumsDatabaseInserter",
    "SpotifyArtistsDatabaseInserter",
    "SpotifyAudioFeaturesDatabaseInserter",
    "SpotifyTracksDatabaseInserter",
    "TrackIDMappingDatabaseInserter",
    "BaseSpotifyDatabaseInserter",
    "SpotifyInsertionsManager",

    # Shazam
    "ShazamTopTracksDatabaseInserter",
    "ShazamArtistsDatabaseInserter",
    "ShazamTracksDatabaseInserter",
    "ShazamInsertionsManager",

    # Other
    "RadioTracksDatabaseInserter"
]
