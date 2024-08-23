from data_collectors.logic.managers.misc.billboard_manager import BillboardManager
from data_collectors.logic.managers.misc.radio_snapshots_manager import RadioStationsSnapshotsManager
from data_collectors.logic.managers.misc.release_radar_manager import ReleaseRadarManager
from data_collectors.logic.managers.misc.tracks_lyrics_manager import TracksLyricsManager
from data_collectors.logic.managers.misc.tracks_lyrics_missing_ids_manager import TracksLyricsMissingIDsManager
from data_collectors.logic.managers.misc.tracks_vectorizer_manager import TracksVectorizerManager
from data_collectors.logic.managers.misc.wikipedia_artists_age_manager import WikipediaArtistsAgeManager
from data_collectors.logic.managers.misc.artists_images_gender_manager import ArtistsImagesGenderManager

__all__ = [
    "ArtistsImagesGenderManager",
    "BillboardManager",
    "RadioStationsSnapshotsManager",
    "ReleaseRadarManager",
    "TracksLyricsManager",
    "TracksLyricsMissingIDsManager",
    "TracksVectorizerManager",
    "WikipediaArtistsAgeManager"
]
