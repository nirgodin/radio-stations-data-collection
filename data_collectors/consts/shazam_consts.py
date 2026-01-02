from data_collectors.logic.models import WebElement, HTMLElement

ISRAEL_COUNTRY_CODE = "IL"
KEY = "key"
ADAM_ID = "adamid"
HITS = "hits"
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
SHAZAM_TOP_TRACKS_CSS_SELECTOR = "body > div:nth-child(3) > div:nth-child(1) > main > div.PageGrid-module_grid__SjmKd.PageGrid-module_right__1113p.PageGrid-module_fullWidth__c9UaO > div > div > div > div.ListShowMoreLess_container__t4TNB.page_chartList__aBclW"
SHAZAM_TOP_TRACK_WEB_ELEMENT = WebElement(
    name="shazam_top_track", type=HTMLElement.A, class_="common_link__7If7r", multiple=True
)
DATA = "data"
ATTRIBUTES = "attributes"
ARTIST_BIO = "artistBio"
ORIGIN = "origin"
BORN_OR_FORMED = "bornOrFormed"
GENRE_NAMES = "genreNames"
VIEWS = "views"
SIMILAR_ARTISTS = "similar-artists"
PRIMARY = "primary"
SECTIONS = "sections"
METADATA = "metadata"
TEXT = "text"
LABEL = "Label"
LYRICS_FOOTER = "footer"
TYPE = "type"
SHAZAM_LYRICS = "LYRICS"
