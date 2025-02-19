from genie_datastores.postgres.models import Chart

POSITION_COLUMN_NAME = b"\xd7\x9e\xd7\xa7\xd7\x95\xd7\x9d".decode("utf-8")
SONG_COLUMN_NAME = b"\xd7\xa9\xd7\x99\xd7\xa8".decode("utf-8")
ARTIST_COLUMN_NAME = b"\xd7\x91\xd7\x99\xd7\xa6\xd7\x95\xd7\xa2".decode("utf-8")
PERFORMER_COLUMN_NAME = b"\xd7\x9e\xd7\x91\xd7\xa6\xd7\xa2".decode("utf-8")
CHART_RELEVANT_COLUMNS = [
    POSITION_COLUMN_NAME,
    SONG_COLUMN_NAME,
    ARTIST_COLUMN_NAME,
    PERFORMER_COLUMN_NAME,
]
RADIO_CHART_SHEET_NAME_DATETIME_FORMATS = ["%d.%m.%y"]
EXCLUDED_RADIO_CHARTS_FILES_IDS = [
    "1zJsTgxYkuW6c0TAqVaRI_RMToTTH7gn-",
    "1z8Ys_yQDg3cjhj3uWUaKFVLIk8xB-qmi",
    "1jsAon5Fl_Tp7KoH_Bu0WAnudXe2FMcPp",
    "1shGMCB6uhQeNpqepPVBdPJWFG0IVAbPr",
    "1of9J-7k3EqEzvgH8mZXqGv5N-oAIC30d",
    "1XSReUm0scWptWpBnxeDLuLpfGRwaPNPA",
]
CHART_KEY_FORMAT = "{artist} - {track}"
SPOTIFY_PLAYLIST_CHART_MAP = {
    "37i9dQZEVXbJ6IpvItkve3": Chart.SPOTIFY_DAILY_ISRAELI,
    "37i9dQZEVXbMDoHDwVN2tF": Chart.SPOTIFY_DAILY_INTERNATIONAL,
}
MAKO_PLAYLIST_CHART_MAP = {
    "3Oh3oSaZjfsXcNwSpVMye2": Chart.MAKO_WEEKLY_HIT_LIST,
}
BILLBOARD_PLAYLIST_CHART_MAP = {
    "6UeSakyzhiEt4NB3UAd6NQ": Chart.BILLBOARD_HOT_100,
}
