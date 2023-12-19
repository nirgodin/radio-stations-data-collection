from typing import Set, Optional

from data_collectors.consts.spotify_consts import TRACK, ARTISTS, ID


def extract_unique_artists_ids(*tracks: dict) -> Set[str]:
    artists_ids = set()

    for track in tracks:
        artist_id = _extract_single_artist_id(track)

        if artist_id is not None:
            artists_ids.add(artist_id)

    return artists_ids


def _extract_single_artist_id(track: dict) -> Optional[str]:
    inner_track = track.get(TRACK, {})
    if inner_track is None:
        return

    artists = inner_track.get(ARTISTS, [])
    if not artists:
        return

    return artists[0][ID]
