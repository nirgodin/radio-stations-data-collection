from typing import Generator

from genie_common.utils import safe_nested_get

from data_collectors.consts.spotify_consts import ID, TRACKS, ITEMS, TRACK
from data_collectors.logic.managers.spotify_playlists.base_spotify_playlists_manager import BaseSpotifyPlaylistsManager


class SpotifyPlaylistsTracksManager(BaseSpotifyPlaylistsManager):
    def _extract_single_playlist_ids(self, playlist: dict) -> Generator[str, None, None]:
        playlist_tracks = safe_nested_get(playlist, [TRACKS, ITEMS], [])

        for track in playlist_tracks:
            inner_track = track.get(TRACK)

            if isinstance(inner_track, dict):
                track_id = inner_track.get(ID)

                if track_id:
                    yield track_id
