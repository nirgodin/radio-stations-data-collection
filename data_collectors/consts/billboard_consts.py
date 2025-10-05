from datetime import datetime

from data_collectors.logic.models import HTMLElement, WebElement

BILLBOARD_DATETIME_FORMAT = "%Y-%m-%d"
FIRST_BILLBOARD_CHART_DATE = datetime(1958, 8, 4)
BILLBOARD_HOT_100_CHART_NAME = "hot-100"
SONG_LIST_ITEM_WEB_ELEMENT = WebElement(
    name="a",
    type=HTMLElement.LI,
    class_="o-chart-results-list__item // lrv-u-flex-grow-1 lrv-u-flex lrv-u-flex-direction-column lrv-u-justify-content-center lrv-u-padding-l-050 lrv-u-padding-l-00@mobile-max u-max-width-397",
    multiple=True,
    enumerate=False,
)
