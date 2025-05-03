from enum import Enum


class JobId(str, Enum):
    BILLBOARD_CHARTS = "billboard_charts"
    EUROVISION_CHARTS = "eurovision_charts"
    FEATURED_ARTISTS_IMPUTER = "featured_artists_imputer"
    GLGLZ_CHARTS = "glglz_charts"
    MAKO_HIT_LIST = "mako_hit_list"
    RADIO_SNAPSHOTS = "radio_snapshots"
    SHAZAM_TOP_TRACKS = "shazam_top_tracks"
    SPOTIFY_CHARTS = "spotify_charts"
    STATUS_REPORTER = "status_reporter"
