from datetime import datetime
from urllib.parse import unquote

from data_collectors.consts.glglz_consts import GLGLZ_WEEKLY_CHART_URL_FORMAT


def generate_chart_date_url(date: datetime, datetime_format: str, should_unquote: bool = False) -> str:
    formatted_date = date.strftime(datetime_format).strip()
    url = GLGLZ_WEEKLY_CHART_URL_FORMAT.format(date=formatted_date)

    return unquote(url) if should_unquote else url
