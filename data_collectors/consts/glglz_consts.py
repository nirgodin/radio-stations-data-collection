from data_collectors.logic.models import HTMLElement, WebElement

WEEKLY_CHART_PREFIX = (
    b"\xd7\x94\xd7\x9e\xd7\xa6\xd7\xa2\xd7\x93 \xd7\x94\xd7\xa9\xd7\x91\xd7\x95\xd7\xa2\xd7\x99".decode("utf-8")
)
GLGLZ_CHARTS_ARCHIVE_URL = "https://glz.co.il/%D7%92%D7%9C%D7%92%D7%9C%D7%A6/%D7%9B%D7%AA%D7%91%D7%95%D7%AA/%D7%90%D7%A8%D7%9B%D7%99%D7%95%D7%9F-%D7%9E%D7%A6%D7%A2%D7%93%D7%99%D7%9D"
GLGLZ_CHARTS_LINKS_WEB_ELEMENT = WebElement(name="glglz_charts_links", type=HTMLElement.A, multiple=True)
UNAVAILABLE_GLGLZ_CHART_SUBSTRINGS = ["custom 404", "is temporarily unavailable"]
