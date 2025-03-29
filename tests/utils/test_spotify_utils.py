from typing import List

from genie_common.utils import random_string_dict
from spotipyio.testing import SpotifyMockFactory

from data_collectors.consts.spotify_consts import TRACK
from data_collectors.utils.spotify import extract_unique_artists_ids


class TestSpotifyUtils:
    def test_extract_unique_artists_ids__tracks_without_artists__returns_empty_set(self):
        tracks_without_artists = [random_string_dict(), SpotifyMockFactory.track(artists=[])]
        actual = extract_unique_artists_ids(*tracks_without_artists)
        assert actual == set()

    def test_extract_unique_artists_ids__tracks_with_artists__returns_expected_artists(self):
        track_a_artists_ids = SpotifyMockFactory.some_spotify_ids()
        track_a = self._a_track_with_artists_ids(track_a_artists_ids)
        track_b_artists_ids = SpotifyMockFactory.some_spotify_ids()
        track_b = self._a_track_with_artists_ids(track_b_artists_ids)
        expected = set(track_a_artists_ids) | set(track_b_artists_ids)

        actual = extract_unique_artists_ids(track_a, track_b)

        assert actual == expected

    def test_extract_unique_artists_ids__duplicate_artists__returns_unique_artists(self):
        artists_ids = SpotifyMockFactory.some_spotify_ids()
        track_a = self._a_track_with_artists_ids(artists_ids)
        track_b = self._a_track_with_artists_ids(artists_ids)

        actual = extract_unique_artists_ids(track_a, track_b)

        assert actual == set(artists_ids)

    @staticmethod
    def _a_track_with_artists_ids(ids: List[str]) -> dict:
        artists = [SpotifyMockFactory.artist(id=artist_id) for artist_id in ids]
        return {TRACK: SpotifyMockFactory.track(artists=artists)}
