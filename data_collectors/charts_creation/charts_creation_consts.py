from data_collectors.charts_creation.charts_creation_utils import get_resource_path
from data_collectors.charts_creation.covers.image_text import Font, ImageText

FONT_PATH = get_resource_path("MPLUSRounded1c-ExtraBold.ttf")
WEEKLY_CHART_IMAGE_TEXT = ImageText(
    font=Font(path=FONT_PATH, size=130),
    text=b"\xd7\x94\xd7\x9e\xd7\xa6\xd7\xa2\xd7\x93 \xd7\x94\xd7\xa9\xd7\x91\xd7\x95\xd7\xa2\xd7\x99".decode("utf-8"),
    position=(170, 770),
    color="#ffd61c",
)
INTERNATIONAL_IMAGE_TEXT = ImageText(
    font=Font(path=FONT_PATH, size=90),
    text=b"\xd7\x91\xd7\x99\xd7\xa0\xd7\x9c\xd7\x90\xd7\x95\xd7\x9e\xd7\x99".decode("utf-8"),
    position=(665, 920),
    color="#f71c1c",
)
ISRAELI_IMAGE_TEXT = ImageText(
    font=Font(path=FONT_PATH, size=90),
    text=b"\xd7\x99\xd7\xa9\xd7\xa8\xd7\x90\xd7\x9c\xd7\x99".decode("utf-8"),
    position=(750, 920),
    color="#1cacff",
)
DATE_FONT_SIZE = 60
DATE_POSITION = (180, 950)
DATE_COLOR = "#ffffff"
PLAYLIST_NAME_FORMAT = b"\xd7\x94\xd7\x9e\xd7\xa6\xd7\xa2\xd7\x93 {chart_type} \xd7\xa9\xd7\x9c \xd7\x92\xd7\x9c\xd7\x92\xd7\x9c\xd7\xa6 | {chart_date}".decode(
    "utf-8"
)
