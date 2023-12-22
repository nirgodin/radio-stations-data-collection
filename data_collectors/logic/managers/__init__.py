from data_collectors.logic.managers.artists_images_gender_manager import ArtistsImagesGenderManager
from data_collectors.logic.managers.billboard_manager import BillboardManager
from data_collectors.logic.managers.google_artists_origin_geocoding_manager import GoogleArtistsOriginGeocodingManager
from data_collectors.logic.managers.missing_ids_managers.genius_missing_ids_manager import GeniusMissingIDsManager
from data_collectors.logic.managers.missing_ids_managers.musixmatch_missing_ids_manager import \
    MusixmatchMissingIDsManager
from data_collectors.logic.managers.radio_snapshots_manager import RadioStationsSnapshotsManager
from data_collectors.logic.managers.missing_ids_managers.shazam_missing_ids_manager import ShazamMissingIDsManager
from data_collectors.logic.managers.shazam_birth_date_copy_manager import ShazamBirthDateCopyManager
from data_collectors.logic.managers.shazam_top_tracks_manager import ShazamTopTracksManager
from data_collectors.logic.managers.spotify_playlists_artists_manager import SpotifyPlaylistsArtistsManager
from data_collectors.logic.managers.track_names_embeddings_manager import TrackNamesEmbeddingsManager
from data_collectors.logic.managers.wikipedia_artists_age_manager import WikipediaArtistsAgeManager

__all__ = [
    "ArtistsImagesGenderManager",
    "BillboardManager",
    "GeniusMissingIDsManager",
    "GoogleArtistsOriginGeocodingManager",
    "MusixmatchMissingIDsManager",
    "RadioStationsSnapshotsManager",
    "ShazamTopTracksManager",
    "ShazamMissingIDsManager",
    "ShazamBirthDateCopyManager",
    "SpotifyPlaylistsArtistsManager",
    "TrackNamesEmbeddingsManager",
    "WikipediaArtistsAgeManager"
]
