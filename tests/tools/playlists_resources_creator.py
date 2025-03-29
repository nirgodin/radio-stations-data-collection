from typing import List, Dict

from spotipyio.testing import SpotifyMockFactory

from tests.helpers.spotify_playlists_resources import SpotifyPlaylistsResources


class PlaylistsResourcesCreator:
    @staticmethod
    def create(ids: List[str]) -> Dict[str, SpotifyPlaylistsResources]:
        return {playlist_id: PlaylistsResourcesCreator.create_single(playlist_id) for playlist_id in ids}

    @staticmethod
    def create_single(playlist_id: str) -> SpotifyPlaylistsResources:
        playlist = SpotifyMockFactory.playlist(id=playlist_id)
        items = playlist["tracks"]["items"]
        tracks = {item["track"]["id"]: item for item in items}
        artists = PlaylistsResourcesCreator._build_artists_map(items)
        albums = {item["track"]["album"]["id"]: item["track"]["album"] for item in items}

        return SpotifyPlaylistsResources(
            id=playlist_id,
            playlist=playlist,
            tracks=tracks,
            artists=artists,
            albums=albums,
        )

    @staticmethod
    def _build_artists_map(items: List[dict]) -> Dict[str, dict]:
        artists_map = {}

        for item in items:
            artists = item["track"]["artists"]

            for artist in artists:
                artists_map[artist["id"]] = artist

        return artists_map
