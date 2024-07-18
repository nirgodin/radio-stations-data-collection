from typing import Generator

from genie_common.utils import safe_nested_get
from genie_datastores.postgres.inner_utils.spotify_utils import extract_artist_id

from data_collectors.consts.spotify_consts import TRACKS, ITEMS, TRACK
from data_collectors.logic.managers.spotify.playlists.base_spotify_playlists_manager import BaseSpotifyPlaylistsManager


class SpotifyPlaylistsArtistsManager(BaseSpotifyPlaylistsManager):
    def _extract_single_playlist_ids(self, playlist: dict) -> Generator[str, None, None]:
        playlist_tracks = safe_nested_get(playlist, [TRACKS, ITEMS], [])

        for track in playlist_tracks:
            inner_track = track.get(TRACK, {})

            if inner_track:
                artist_id = extract_artist_id(inner_track)

                if artist_id:
                    yield artist_id
