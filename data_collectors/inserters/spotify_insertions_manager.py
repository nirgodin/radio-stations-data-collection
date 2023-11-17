from typing import List, Dict

from postgres_client import BaseSpotifyORMModel
from data_collectors.inserters.spotify_database_inserters import *


class SpotifyInsertionsManager:
    def __init__(self,
                 artists_database_inserter: SpotifyArtistsDatabaseInserter,
                 albums_database_inserter: SpotifyAlbumsDatabaseInserter,
                 tracks_database_inserter: SpotifyTracksDatabaseInserter,
                 audio_features_database_inserter: SpotifyAudioFeaturesDatabaseInserter,
                 track_id_mapping_database_inserter: TrackIDMappingDatabaseInserter):
        self._artists_database_inserter = artists_database_inserter
        self._albums_database_inserter = albums_database_inserter
        self._tracks_database_inserter = tracks_database_inserter
        self._audio_features_database_inserter = audio_features_database_inserter
        self._track_id_mapping_database_inserter = track_id_mapping_database_inserter

    async def insert(self, tracks: List[dict]) -> Dict[str, List[BaseSpotifyORMModel]]:
        spotify_records = {}

        for inserter in self._ordered_database_inserters:
            records = await inserter.insert(tracks)
            spotify_records[inserter.name] = records

        return spotify_records

    @property
    def _ordered_database_inserters(self) -> List[BaseSpotifyDatabaseInserter]:
        return [
            self._artists_database_inserter,
            self._albums_database_inserter,
            self._tracks_database_inserter,
            self._audio_features_database_inserter,
            self._track_id_mapping_database_inserter
        ]
