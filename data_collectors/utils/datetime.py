from datetime import datetime, timedelta
from random import randint

from data_collectors.logic.models.week_day import WeekDay


def random_upcoming_time() -> datetime:
    return datetime.now() + timedelta(minutes=randint(1, 10))


def find_next_weekday(target: WeekDay, hour: int) -> datetime:
    now = datetime.now()
    current = now.weekday()

    if current == target.value:
        todays_hour = datetime(now.year, now.month, now.day, hour)
        return todays_hour if now.hour < hour else todays_hour + timedelta(days=7)

    days_delta = target.value - current
    return datetime(now.year, now.month, now.day + days_delta, hour)
