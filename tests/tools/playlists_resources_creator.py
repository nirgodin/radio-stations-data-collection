from typing import List, Dict

from spotipyio.testing import SpotifyMockFactory

from data_collectors.utils.spotify import extract_unique_artists_ids
from tests.helpers.spotify_playlists_resources import SpotifyPlaylistsResources


class PlaylistsResourcesCreator:
    @staticmethod
    def create(ids: List[str]) -> Dict[str, SpotifyPlaylistsResources]:
        return {
            playlist_id: PlaylistsResourcesCreator.create_single(playlist_id)
            for playlist_id in ids
        }

    @staticmethod
    def create_single(playlist_id: str) -> SpotifyPlaylistsResources:
        playlist = SpotifyMockFactory.playlist(id=playlist_id)
        items = playlist["tracks"]["items"]
        tracks = [item["track"]["id"] for item in items]
        artists = extract_unique_artists_ids(*items)
        albums = [item["track"]["album"]["id"] for item in items]

        return SpotifyPlaylistsResources(
            id=playlist_id,
            playlist=playlist,
            tracks=tracks,
            artists=list(artists),
            albums=albums,
        )
