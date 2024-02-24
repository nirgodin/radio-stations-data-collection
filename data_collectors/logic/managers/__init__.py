from data_collectors.logic.managers.artists_images_gender_manager import ArtistsImagesGenderManager
from data_collectors.logic.managers.billboard_manager import BillboardManager
from data_collectors.logic.managers.charts.charts_israeli_artists_manager import ChartsIsraeliArtistsManager
from data_collectors.logic.managers.charts.charts_tagged_mistakes_manager import ChartsTaggedMistakesManager
from data_collectors.logic.managers.charts.eurovision_charts_manager import EurovisionChartsManager
from data_collectors.logic.managers.charts.every_hit_charts_manager import EveryHitChartsManager
from data_collectors.logic.managers.charts.glglz_charts_manager import GlglzChartsManager
from data_collectors.logic.managers.charts.playlists_charts_manager import PlaylistsChartsManager
from data_collectors.logic.managers.charts.radio_charts_manager import RadioChartsManager
from data_collectors.logic.managers.genres.genres_mapping_manager import GenresMappingManager
from data_collectors.logic.managers.google_artists_origin_geocoding_manager import GoogleArtistsOriginGeocodingManager
from data_collectors.logic.managers.missing_ids_managers.genius_missing_ids_manager import GeniusMissingIDsManager
from data_collectors.logic.managers.missing_ids_managers.musixmatch_missing_ids_manager import \
    MusixmatchMissingIDsManager
from data_collectors.logic.managers.missing_ids_managers.shazam_missing_ids_manager import ShazamMissingIDsManager
from data_collectors.logic.managers.radio_snapshots_manager import RadioStationsSnapshotsManager
from data_collectors.logic.managers.shazam.shazam_top_tracks_manager import ShazamTopTracksManager
from data_collectors.logic.managers.spotify_playlists.spotify_playlists_artists_manager import \
    SpotifyPlaylistsArtistsManager
from data_collectors.logic.managers.spotify_playlists.spotify_playlists_tracks_manager import \
    SpotifyPlaylistsTracksManager
from data_collectors.logic.managers.track_names_embeddings_manager import TrackNamesEmbeddingsManager
from data_collectors.logic.managers.tracks_lyrics_manager import TracksLyricsManager
from data_collectors.logic.managers.tracks_lyrics_missing_ids_manager import TracksLyricsMissingIDsManager
from data_collectors.logic.managers.wikipedia_artists_age_manager import WikipediaArtistsAgeManager

__all__ = [
    "ArtistsImagesGenderManager",
    "BillboardManager",
    "ChartsTaggedMistakesManager",
    "ChartsIsraeliArtistsManager",
    "EurovisionChartsManager",
    "EveryHitChartsManager",
    "GeniusMissingIDsManager",
    "GenresMappingManager",
    "GlglzChartsManager",
    "GoogleArtistsOriginGeocodingManager",
    "MusixmatchMissingIDsManager",
    "RadioChartsManager",
    "RadioStationsSnapshotsManager",
    "ShazamTopTracksManager",
    "ShazamMissingIDsManager",
    "ShazamBirthDateCopyManager",
    "PlaylistsChartsManager",
    "PrimaryGenreManager",
    "SpotifyPlaylistsArtistsManager",
    "SpotifyPlaylistsTracksManager",
    "TrackNamesEmbeddingsManager",
    "TracksLyricsManager",
    "TracksLyricsMissingIDsManager",
    "WikipediaArtistsAgeManager"
]
