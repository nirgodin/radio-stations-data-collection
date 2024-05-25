from data_collectors.logic.managers.artists_images_gender_manager import ArtistsImagesGenderManager
from data_collectors.logic.managers.billboard_manager import BillboardManager
from data_collectors.logic.managers.charts.charts_israeli_artists_manager import ChartsIsraeliArtistsManager
from data_collectors.logic.managers.charts.charts_tagged_mistakes_manager import ChartsTaggedMistakesManager
from data_collectors.logic.managers.charts.eurovision_charts_manager import EurovisionChartsManager
from data_collectors.logic.managers.charts.eurovision_missing_tracks_manager import EurovisionMissingTracksManager
from data_collectors.logic.managers.charts.every_hit_charts_manager import EveryHitChartsManager
from data_collectors.logic.managers.charts.glglz_charts_manager import GlglzChartsManager
from data_collectors.logic.managers.charts.playlists_charts_manager import PlaylistsChartsManager
from data_collectors.logic.managers.charts.radio_charts_manager import RadioChartsManager
from data_collectors.logic.managers.embeddings.track_names_embeddings_retrieval_manager import \
    TrackNamesEmbeddingsRetrievalManager
from data_collectors.logic.managers.genres.genres_artists_origin_manager import GenresArtistsOriginManager
from data_collectors.logic.managers.genres.genres_mapping_manager import GenresMappingManager
from data_collectors.logic.managers.genres.primary_genre_manager import PrimaryGenreManager
from data_collectors.logic.managers.google_artists_origin_geocoding_manager import GoogleArtistsOriginGeocodingManager
from data_collectors.logic.managers.translations.base_translation_manager import BaseTranslationManager
from data_collectors.logic.managers.translations.shazam_israeli_artists_translation_manager import \
    ShazamIsraeliArtistsTranslationManager
from data_collectors.logic.managers.translations.spotify_israeli_artists_translation_manager import \
    SpotifyIsraeliArtistsTranslationManager
from data_collectors.logic.managers.missing_ids_managers.genius_missing_ids_manager import GeniusMissingIDsManager
from data_collectors.logic.managers.missing_ids_managers.musixmatch_missing_ids_manager import \
    MusixmatchMissingIDsManager
from data_collectors.logic.managers.missing_ids_managers.shazam_missing_ids_manager import ShazamMissingIDsManager
from data_collectors.logic.managers.radio_snapshots_manager import RadioStationsSnapshotsManager
from data_collectors.logic.managers.shazam.shazam_birth_date_copy_manager import ShazamBirthDateCopyManager
from data_collectors.logic.managers.shazam.shazam_origin_copy_manager import ShazamOriginCopyManager
from data_collectors.logic.managers.shazam.shazam_top_tracks_manager import ShazamTopTracksManager
from data_collectors.logic.managers.spotify_playlists.spotify_playlists_artists_manager import \
    SpotifyPlaylistsArtistsManager
from data_collectors.logic.managers.spotify_playlists.spotify_playlists_tracks_manager import \
    SpotifyPlaylistsTracksManager
from data_collectors.logic.managers.embeddings.track_names_embeddings_manager import TrackNamesEmbeddingsManager
from data_collectors.logic.managers.tracks_lyrics_manager import TracksLyricsManager
from data_collectors.logic.managers.tracks_lyrics_missing_ids_manager import TracksLyricsMissingIDsManager
from data_collectors.logic.managers.tracks_vectorizer_manager import TracksVectorizerManager
from data_collectors.logic.managers.translations.spotify_israeli_tracks_translation_manager import \
    SpotifyIsraeliTracksTranslationManager
from data_collectors.logic.managers.wikipedia_artists_age_manager import WikipediaArtistsAgeManager

__all__ = [
    "ArtistsImagesGenderManager",
    "BaseTranslationManager",
    "BillboardManager",
    "ChartsTaggedMistakesManager",
    "ChartsIsraeliArtistsManager",
    "EurovisionChartsManager",
    "EurovisionMissingTracksManager",
    "EveryHitChartsManager",
    "GeniusMissingIDsManager",
    "GenresArtistsOriginManager",
    "GenresMappingManager",
    "GlglzChartsManager",
    "GoogleArtistsOriginGeocodingManager",
    "MusixmatchMissingIDsManager",
    "RadioChartsManager",
    "RadioStationsSnapshotsManager",
    "ShazamTopTracksManager",
    "ShazamMissingIDsManager",
    "ShazamBirthDateCopyManager",
    "ShazamOriginCopyManager",
    "PlaylistsChartsManager",
    "PrimaryGenreManager",
    "ShazamIsraeliArtistsTranslationManager",
    "SpotifyIsraeliArtistsTranslationManager",
    "SpotifyIsraeliTracksTranslationManager",
    "SpotifyPlaylistsArtistsManager",
    "SpotifyPlaylistsTracksManager",
    "TrackNamesEmbeddingsManager",
    "TrackNamesEmbeddingsRetrievalManager",
    "TracksLyricsManager",
    "TracksLyricsMissingIDsManager",
    "TracksVectorizerManager",
    "WikipediaArtistsAgeManager"
]
