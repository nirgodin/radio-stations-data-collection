from genie_datastores.postgres.models import Chart

from data_collectors.logic.models import HTMLElement, WebElement

WEEKLY_CHART_PREFIX = (
    b"\xd7\x94\xd7\x9e\xd7\xa6\xd7\xa2\xd7\x93 \xd7\x94\xd7\xa9\xd7\x91\xd7\x95\xd7\xa2\xd7\x99".decode("utf-8")
)
GLGLZ_CHARTS_ARCHIVE_URL = "https://glz.co.il/%D7%92%D7%9C%D7%92%D7%9C%D7%A6/%D7%9B%D7%AA%D7%91%D7%95%D7%AA/%D7%90%D7%A8%D7%9B%D7%99%D7%95%D7%9F-%D7%9E%D7%A6%D7%A2%D7%93%D7%99%D7%9D"
GLGLZ_CHARTS_LINKS_WEB_ELEMENT = WebElement(name="glglz_charts_links", type=HTMLElement.A, multiple=True)
UNAVAILABLE_GLGLZ_CHART_SUBSTRINGS = ["custom 404", "is temporarily unavailable"]
GLZ_CHART_TO_URL_MAP = {
    Chart.GLGLZ_WEEKLY_INTERNATIONAL: "https://glz.co.il/%D7%92%D7%9C%D7%92%D7%9C%D7%A6/%D7%9E%D7%A6%D7%A2%D7%93%D7%99%D7%9D/%D7%94%D7%9E%D7%A6%D7%A2%D7%93-%D7%94%D7%91%D7%99%D7%A0%D7%9C%D7%90%D7%95%D7%9E%D7%99",
    Chart.GLGLZ_WEEKLY_ISRAELI: "https://glz.co.il/%D7%92%D7%9C%D7%92%D7%9C%D7%A6/%D7%9E%D7%A6%D7%A2%D7%93%D7%99%D7%9D/%D7%94%D7%9E%D7%A6%D7%A2%D7%93-%D7%94%D7%99%D7%A9%D7%A8%D7%90%D7%9C%D7%99",
}
WEEKLY_CHART_DATE_SUB_TITLE_ELEMENT = WebElement(
    name="chart_title",
    type=HTMLElement.H3,
    class_="mainTitle",
    child_element=WebElement(name="chart_date", type=HTMLElement.SPAN, class_="sub"),
)
CHART_ENTRY_WEB_ELEMENT_NAME = "chart_entry"
CHART_ENTRY_WEB_ELEMENT_TYPE = HTMLElement.DIV
CHART_ENTRY_WEB_ELEMENT_CLASS = "content row"
WEEKLY_CHART_ARTIST_ELEMENT = WebElement(
    name=CHART_ENTRY_WEB_ELEMENT_NAME,
    type=CHART_ENTRY_WEB_ELEMENT_TYPE,
    class_=CHART_ENTRY_WEB_ELEMENT_CLASS,
    multiple=True,
    child_element=WebElement(name="artist_name", type=HTMLElement.DIV, class_="name"),
)
WEEKLY_CHART_SONG_ELEMENT = WebElement(
    name=CHART_ENTRY_WEB_ELEMENT_NAME,
    type=CHART_ENTRY_WEB_ELEMENT_TYPE,
    class_=CHART_ENTRY_WEB_ELEMENT_CLASS,
    multiple=True,
    child_element=WebElement(name="song_name", type=HTMLElement.A, class_="info"),
)
DD_MM_YYYY_DATETIME_REGEX = r"\b\d{2}/\d{2}/\d{4}\b"
CHART_TITLE_CSS_SELECTOR = "#tabLink1 > div.titleContainer > h3"
GLGLZ_CHARTS_DATETIME_FORMAT = "%d/%m/%Y"
