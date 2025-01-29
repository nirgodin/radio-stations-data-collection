from enum import Enum


class JobId(str, Enum):
    RADIO_SNAPSHOTS = "radio_snapshots"
    SHAZAM_TOP_TRACKS = "shazam_top_tracks"
    SPOTIFY_CHARTS = "spotify_charts"
    STATUS_REPORTER = "status_reporter"
