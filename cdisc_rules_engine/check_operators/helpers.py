from datetime import datetime
import numpy as np
from dateutil.parser import parse
import pytz


def get_year(date_string: str):
    timestamp = get_date(date_string)
    return timestamp.year


def get_month(date_string: str):
    timestamp = get_date(date_string)
    return timestamp.month


def get_day(date_string: str):
    timestamp = get_date(date_string)
    return timestamp.day


def get_hour(date_string: str):
    timestamp = get_date(date_string)
    return timestamp.hour


def get_minute(date_string: str):
    timestamp = get_date(date_string)
    return timestamp.minute


def get_second(date_string: str):
    timestamp = get_date(date_string)
    return timestamp.second


def get_microsecond(date_string: str):
    timestamp = get_date(date_string)
    return timestamp.microsecond


def get_date_component(component: str, date_string: str):
    component_func_map = {
        "year": get_year,
        "month": get_month,
        "day": get_day,
        "hour": get_hour,
        "minute": get_minute,
        "microsecond": get_microsecond,
        "second": get_second,
    }
    component_function = component_func_map.get(component)
    if component_function:
        return component_function(date_string)
    else:
        return get_date(date_string)


def get_date(date_string: str):
    """
    Returns a utc timestamp for comparison
    """
    date = parse(date_string, default=datetime(1970, 1, 1))
    utc = pytz.UTC
    if date.tzinfo is not None and date.tzinfo.utcoffset(date) is not None:
        # timezone aware
        return date.astimezone(utc)
    else:
        return utc.localize(date)


def compare_dates(component, target, comparator, operator):
    if not target or not comparator:
        # Comparison should return false if either is empty or None
        return False
    else:
        return operator(
            get_date_component(component, target),
            get_date_component(component, comparator),
        )


vectorized_compare_dates = np.vectorize(compare_dates)
