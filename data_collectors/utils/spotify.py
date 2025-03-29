from typing import Set, List

from data_collectors.consts.spotify_consts import TRACK, ARTISTS, ID


def extract_unique_artists_ids(*tracks: dict) -> Set[str]:
    artists_ids = set()

    for track in tracks:
        track_artists = _extract_single_artist_id(track)

        for artist_id in track_artists:
            artists_ids.add(artist_id)

    return artists_ids


def get_track_artists(track: dict) -> List[dict]:
    return track.get(TRACK, {}).get(ARTISTS, [])


def _extract_single_artist_id(track: dict) -> List[str]:
    artists = get_track_artists(track)
    return [artist[ID] for artist in artists]
