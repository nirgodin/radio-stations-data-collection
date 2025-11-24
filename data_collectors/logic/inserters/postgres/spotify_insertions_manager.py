from typing import List, Dict

from genie_common.utils import safe_nested_get, chain_lists
from genie_datastores.postgres.models import BaseORMModel, SpotifyFeaturedArtist

from data_collectors.consts.spotify_consts import TRACK, ID
from data_collectors.logic.inserters.postgres import BaseIDsDatabaseInserter
from data_collectors.logic.inserters.postgres.spotify import *
from data_collectors.utils.spotify import get_track_artists


class SpotifyInsertionsManager:
    def __init__(
        self,
        spotify_artists_inserter: SpotifyArtistsDatabaseInserter,
        albums_inserter: SpotifyAlbumsDatabaseInserter,
        spotify_tracks_inserter: SpotifyTracksDatabaseInserter,
        audio_features_inserter: SpotifyAudioFeaturesDatabaseInserter,
        track_id_mapping_inserter: TrackIDMappingDatabaseInserter,
        artists_inserter: ArtistsDatabaseInserter,
        tracks_inserter: TracksDatabaseInserter,
        featured_artists_inserter: SpotifyFeaturedArtistsDatabaseInserter,
    ):
        self._spotify_artists_inserter = spotify_artists_inserter
        self._albums_inserter = albums_inserter
        self._spotify_tracks_inserter = spotify_tracks_inserter
        self._audio_features_inserter = audio_features_inserter
        self._track_id_mapping_inserter = track_id_mapping_inserter
        self._artists_inserter = artists_inserter
        self._tracks_inserter = tracks_inserter
        self._featured_artists_inserter = featured_artists_inserter

    async def insert(self, tracks: List[dict]) -> Dict[str, List[BaseORMModel]]:
        spotify_records = {}

        for inserter in self._ordered_database_inserters:
            records = await inserter.insert(tracks)
            spotify_records[inserter.name] = records

        featured_artists = self._build_featured_artists_records(tracks)
        spotify_records["featured_artists"] = featured_artists
        await self._featured_artists_inserter.insert(featured_artists)

        return spotify_records

    def _build_featured_artists_records(self, tracks: List[dict]) -> List[SpotifyFeaturedArtist]:
        records: List[List[SpotifyFeaturedArtist]] = [self._to_featured_artists(track) for track in tracks]
        return chain_lists(records)

    @staticmethod
    def _to_featured_artists(track: dict) -> List[SpotifyFeaturedArtist]:
        track_id = safe_nested_get(track, [TRACK, ID])
        if track_id is None:
            return []

        raw_artists = get_track_artists(track)
        featured_artists = raw_artists[1:]

        return [
            SpotifyFeaturedArtist(track_id=track_id, artist_id=artist[ID], position=i + 1)
            for i, artist in enumerate(featured_artists)
        ]

    @property
    def _ordered_database_inserters(self) -> List[BaseIDsDatabaseInserter]:
        return [
            self._spotify_artists_inserter,
            self._albums_inserter,
            self._spotify_tracks_inserter,
            self._audio_features_inserter,
            self._track_id_mapping_inserter,
            self._artists_inserter,
            self._tracks_inserter,
        ]
