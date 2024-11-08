import math

import datetime as dt


def generate_month_list(now: dt.datetime, then: dt.datetime) -> list[tuple[int, int]]:
    """Generates a list of year-month strings spanning from then to now"""

    month_list: list[tuple[int, int]] = []

    year: int = then.year
    month: int = then.month
    finished: bool = False

    while not finished:
        month_list.append((year, month))
        month = (month + 1) % 13
        if month == 0:
            year += 1
            month = 1
        if month > now.month and year >= now.year:
            finished = True

    return month_list


def calculate_age(date_str: str, date_format: str) -> str:
    """Calculate the age string for the given date string"""

    then: dt.datetime = dt.datetime.strptime(date_str, date_format).replace(
        tzinfo=dt.timezone.utc
    )
    now: dt.datetime = dt.datetime.now(dt.timezone.utc)
    diff: dt.timedelta = now - then

    if diff.days < 7:
        return f"{diff.days} days"

    if diff.days > 7 and diff.days < 365:
        weeks: int = int(round(diff.days / 7, 0))
        return f"{weeks} weeks"

    years: int = math.floor(diff.days / 365)
    weeks_remaining: int = int(round(diff.days / 7 % 52, 0))
    return f"{years} years {weeks_remaining} weeks"
