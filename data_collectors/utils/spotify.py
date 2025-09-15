from typing import Set, List, Optional

from genie_common.utils import safe_nested_get

from data_collectors.consts.spotify_consts import TRACK, ARTISTS, ID


def extract_unique_artists_ids(*tracks: Optional[dict]) -> Set[str]:
    artists_ids = set()

    for track in tracks:
        track_artists = _extract_single_artist_id(track)

        for artist_id in track_artists:
            artists_ids.add(artist_id)

    return artists_ids


def get_track_artists(track: Optional[dict]) -> List[dict]:
    if isinstance(track, dict):
        return safe_nested_get(track, [TRACK, ARTISTS], [])

    return []


def _extract_single_artist_id(track: Optional[dict]) -> List[str]:
    artists = get_track_artists(track)
    return [artist[ID] for artist in artists]
