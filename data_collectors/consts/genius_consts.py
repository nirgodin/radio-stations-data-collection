import re

from data_collectors.logic.models import WebElement, HTMLElement

GENIUS_BASE_URL = 'https://api.genius.com'
GENIUS_SEARCH_URL = f"{GENIUS_BASE_URL}/search"
META = 'meta'
STATUS = 'status'
RESPONSE = 'response'
RESULT = 'result'
PRIMARY_ARTIST = "primary_artist"
GENIUS_LYRICS_URL_FORMAT = 'https://genius.com/{id}'
GENIUS_LYRICS_ELEMENT_NAME = "genius_lyrics_line"
GENIUS_LYRICS_WEB_ELEMENT = WebElement(
    name="genius_lyrics",
    type=HTMLElement.DIV,
    class_=re.compile("^lyrics$|Lyrics__Root"),
    child_element=WebElement(
        name=GENIUS_LYRICS_ELEMENT_NAME,
        type=HTMLElement.DIV,
        class_="Lyrics__Container-sc-1ynbvzw-1 kUgSbL",
        expected_type=list
    ),
)
INVALID_LYRICS_ROWS = [
    "contributor",
    "contributors",
    "embed"
]
