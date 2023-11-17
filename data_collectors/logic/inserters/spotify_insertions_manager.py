from typing import List, Dict

from postgres_client import BaseSpotifyORMModel
from data_collectors.logic.inserters.spotify import *


class SpotifyInsertionsManager:
    def __init__(self,
                 artists_inserter: SpotifyArtistsDatabaseInserter,
                 albums_inserter: SpotifyAlbumsDatabaseInserter,
                 tracks_inserter: SpotifyTracksDatabaseInserter,
                 audio_features_inserter: SpotifyAudioFeaturesDatabaseInserter,
                 track_id_mapping_inserter: TrackIDMappingDatabaseInserter):
        self._artists_inserter = artists_inserter
        self._albums_inserter = albums_inserter
        self._tracks_inserter = tracks_inserter
        self._audio_features_inserter = audio_features_inserter
        self._track_id_mapping_inserter = track_id_mapping_inserter

    async def insert(self, tracks: List[dict]) -> Dict[str, List[BaseSpotifyORMModel]]:
        spotify_records = {}

        for inserter in self._ordered_database_inserters:
            records = await inserter.insert(tracks)
            spotify_records[inserter.name] = records

        return spotify_records

    @property
    def _ordered_database_inserters(self) -> List[BaseSpotifyDatabaseInserter]:
        return [
            self._artists_inserter,
            self._albums_inserter,
            self._tracks_inserter,
            self._audio_features_inserter,
            self._track_id_mapping_inserter
        ]
