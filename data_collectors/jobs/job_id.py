from enum import Enum


class JobId(str, Enum):
    ARTISTS_GEOCODING = "artists_geocoding"
    ARTISTS_INSIGHTS = "artists_insights"
    ARTISTS_WEB_PAGES = "artists_web_pages"
    BILLBOARD_CHARTS = "billboard_charts"
    EUROVISION_CHARTS = "eurovision_charts"
    FEATURED_ARTISTS_IMPUTER = "featured_artists_imputer"
    GENIUS_ARTISTS_ABOUTS = "genius_artists_abouts"
    GENIUS_ARTISTS_IDS_IMPUTER = "genius_artists_ids_imputer"
    GLGLZ_CHARTS = "glglz_charts"
    JOSIE_CURATIONS = "josie_curations"
    MAKO_HIT_LIST = "mako_hit_list"
    RADIO_SNAPSHOTS = "radio_snapshots"
    SHAZAM_TOP_TRACKS = "shazam_top_tracks"
    SPOTIFY_ARTISTS_ABOUTS = "spotify_artists_abouts"
    SPOTIFY_CHARTS = "spotify_charts"
    STATUS_REPORTER = "status_reporter"
    WIKIPEDIA_SUMMARIES = "wikipedia_summaries"
