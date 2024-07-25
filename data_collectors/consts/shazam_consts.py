from data_collectors.logic.models import WebElement, HTMLElement

ISRAEL_COUNTRY_CODE = "IL"
KEY = "key"
ADAM_ID = "adamid"
HITS = 'hits'
HEADING = "heading"
TITLE = "title"
SUBTITLE = "subtitle"
SHAZAM_TRACK_URL_FORMAT = "https://www.shazam.com/track/{track_id}"
SHAZAM_LYRICS_WEB_ELEMENT_NAME = "shazam_lyrics"
SHAZAM_LYRICS_WEB_ELEMENT = WebElement(
    name=SHAZAM_LYRICS_WEB_ELEMENT_NAME,
    type=HTMLElement.P,
    class_="lyrics",
)
DATA = "data"
ATTRIBUTES = "attributes"
ARTIST_BIO = "artistBio"
