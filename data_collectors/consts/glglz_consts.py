from datetime import datetime

from data_collectors.tools.web_elements_extractor import WebElement, HTMLElement

GLGLZ_WEEKLY_CHART_URL_FORMAT = "https://glz.co.il/%D7%92%D7%9C%D7%92%D7%9C%D7%A6/%D7%9B%D7%AA%D7%91%D7%95%D7%AA/%D7%94%D7%9E%D7%A6%D7%A2%D7%93-%D7%94%D7%A9%D7%91%D7%95%D7%A2%D7%99-{date}"
GLGLZ_LEGACY_DATETIME_FORMAT = '%#e%#m%Y'
GLGLZ_DATETIME_FORMAT = "%d%m%Y"
GLGLZ_LEGACY_END_DATE = datetime(2014, 2, 27)
ISRAELI_CHART_TITLE = b'\xd7\x94\xd7\x9e\xd7\xa6\xd7\xa2\xd7\x93 \xd7\x94\xd7\x99\xd7\xa9\xd7\xa8\xd7\x90\xd7\x9c\xd7\x99:'.decode("utf-8")
INTERNATIONAL_CHART_TITLE = b'\xd7\x94\xd7\x9e\xd7\xa6\xd7\xa2\xd7\x93 \xd7\x94\xd7\x91\xd7\x99\xd7\xa0\xd7\x9c\xd7\x90\xd7\x95\xd7\x9e\xd7\x99:'.decode("utf-8")
GLGLZ_CHART_ENTRY = "glglz_chart_entry"
GLGLZ_CHARTS_WEB_ELEMENT = WebElement(
    name="glglz_charts_container",
    type=HTMLElement.NG_COMPONENT,
    class_="ng-star-inserted",
    child_element=WebElement(
        name=GLGLZ_CHART_ENTRY,
        type=HTMLElement.P,
        multiple=True,
        enumerate=False
    ),
    multiple=False,
    enumerate=False
)
POSITION_TRACK_NAME_SEPARATOR = "."
FIRST_GLGLZ_CHART_DATE = datetime(2013, 1, 3)
