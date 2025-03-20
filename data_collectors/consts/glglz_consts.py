from datetime import datetime
from itertools import product

GLGLZ_WEEKLY_CHART_URL_FORMAT = "https://glz.co.il/%D7%92%D7%9C%D7%92%D7%9C%D7%A6/%D7%9B%D7%AA%D7%91%D7%95%D7%AA/%D7%94%D7%9E%D7%A6%D7%A2%D7%93-%D7%94%D7%A9%D7%91%D7%95%D7%A2%D7%99-{date}"
GLGLZ_DAY_FORMATS = ["%d", "%#e"]
GLGLZ_MONTH_FORMATS = ["%m", "%#m"]
GLGLZ_YEAR_FORMATS = ["%Y", "%y"]
GLGLZ_DATETIME_FORMATS = ["%Y-%#e%#m"] + [
    f"{day}{month}{year}"
    for day, month, year in product(
        GLGLZ_DAY_FORMATS, GLGLZ_MONTH_FORMATS, GLGLZ_YEAR_FORMATS
    )
]
ISRAELI_CHART_TITLE = b"\xd7\x94\xd7\x9e\xd7\xa6\xd7\xa2\xd7\x93 \xd7\x94\xd7\x99\xd7\xa9\xd7\xa8\xd7\x90\xd7\x9c\xd7\x99:".decode(
    "utf-8"
)
INTERNATIONAL_CHART_TITLE = b"\xd7\x94\xd7\x9e\xd7\xa6\xd7\xa2\xd7\x93 \xd7\x94\xd7\x91\xd7\x99\xd7\xa0\xd7\x9c\xd7\x90\xd7\x95\xd7\x9e\xd7\x99:".decode(
    "utf-8"
)
GLGLZ_CHART_ENTRY = "glglz_chart_entry"
POSITION_TRACK_NAME_SEPARATOR = "."
FIRST_GLGLZ_CHART_DATE = datetime(2013, 1, 3)
CHART_NAME_SIMILARITY_THRESHOLD = 0.8
