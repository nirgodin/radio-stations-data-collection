from typing import List, Dict

from genie_datastores.postgres.models import BaseORMModel

from data_collectors.logic.inserters.postgres.spotify import *
from data_collectors.logic.inserters.postgres.spotify.tracks_database_inserter import TracksDatabaseInserter


class SpotifyInsertionsManager:
    def __init__(self,
                 spotify_artists_inserter: SpotifyArtistsDatabaseInserter,
                 albums_inserter: SpotifyAlbumsDatabaseInserter,
                 spotify_tracks_inserter: SpotifyTracksDatabaseInserter,
                 audio_features_inserter: SpotifyAudioFeaturesDatabaseInserter,
                 track_id_mapping_inserter: TrackIDMappingDatabaseInserter,
                 artists_inserter: ArtistsDatabaseInserter,
                 tracks_inserter: TracksDatabaseInserter):
        self._spotify_artists_inserter = spotify_artists_inserter
        self._albums_inserter = albums_inserter
        self._spotify_tracks_inserter = spotify_tracks_inserter
        self._audio_features_inserter = audio_features_inserter
        self._track_id_mapping_inserter = track_id_mapping_inserter
        self._artists_inserter = artists_inserter
        self._tracks_inserter = tracks_inserter

    async def insert(self, tracks: List[dict]) -> Dict[str, List[BaseORMModel]]:
        spotify_records = {}

        for inserter in self._ordered_database_inserters:
            records = await inserter.insert(tracks)
            spotify_records[inserter.name] = records

        return spotify_records

    @property
    def _ordered_database_inserters(self) -> List[BaseSpotifyDatabaseInserter]:
        return [
            self._spotify_artists_inserter,
            self._albums_inserter,
            self._spotify_tracks_inserter,
            self._audio_features_inserter,
            self._track_id_mapping_inserter,
            self._artists_inserter,
            self._tracks_inserter
        ]
