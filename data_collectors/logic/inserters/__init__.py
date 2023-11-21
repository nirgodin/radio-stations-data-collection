from data_collectors.logic.inserters.billboard import *
from data_collectors.logic.inserters.shazam import *
from data_collectors.logic.inserters.spotify import *
from data_collectors.logic.inserters.spotify_insertions_manager import SpotifyInsertionsManager
from data_collectors.logic.inserters.radio_tracks_database_inserter import RadioTracksDatabaseInserter

__all__ = [
    # Billboard
    "BillboardChartsDatabaseInserter",
    "BillboardTracksDatabaseInserter",
    "BillboardTracksUpdater",

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

    # Other
    "RadioTracksDatabaseInserter"
]
