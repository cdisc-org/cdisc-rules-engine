from datetime import datetime
import re
import numpy as np
from dateutil.parser import parse, isoparse
import pytz
from cdisc_rules_engine.services import logger
import traceback
from functools import lru_cache


# Date regex pattern for validation
date_regex = re.compile(
    r"^((-?[0-9]{4}|-)(-(1[0-2]|0[1-9]|-)(-(3[01]|0[1-9]|[12][0-9]|-)"
    r"(T(2[0-3]|[01][0-9]|-)(:([0-5][0-9]|-)((:([0-5][0-9]|-))?(\.[0-9]+)?"
    r"((Z|[+-](:2[0-3]|[01][0-9]):[0-5][0-9]))?)?)?)?)?)?)(\/((-?[0-9]{4}|-)"
    r"(-(1[0-2]|0[1-9]|-)(-(3[01]|0[1-9]|[12][0-9]|-)(T(2[0-3]|[01][0-9]|-)"
    r"(:([0-5][0-9]|-)((:([0-5][0-9]|-))?(\.[0-9]+)?((Z|[+-](:2[0-3]|[01][0-9])"
    r":[0-5][0-9]))?)?)?)?)?)?))?$"
)


def is_valid_date(date_string: str) -> bool:
    if date_string is None:
        return False
    try:
        isoparse(date_string)
    except Exception as e:
        uncertainty_substrings = ["/", "--", "-:"]
        if any([substr in date_string for substr in uncertainty_substrings]):
            return date_regex.match(date_string) is not None
        else:
            logger.error(
                f"Error with date parsing: {str(e)}, "
                f"traceback: {traceback.format_exc()}"
            )
            return False
    return date_regex.match(date_string) is not None


def is_valid_duration(duration: str, negative) -> bool:
    if not isinstance(duration, str):
        duration = str(duration)
    if negative:
        pattern = (
            r"^[-]?P(?!$)(?:(?:(\d+(?:[.,]\d*)?Y)?[,]?(\d+(?:[.,]\d*)?M)?[,]?"
            r"(\d+(?:[.,]\d*)?D)?[,]?(T(?=\d)(?:(\d+(?:[.,]\d*)?H)?[,]?"
            r"(\d+(?:[.,]\d*)?M)?[,]?(\d+(?:[.,]\d*)?S)?)?)?)|(\d+(?:[.,]\d*)?W))$"
        )
    else:
        pattern = (
            r"^P(?!$)(?:(?:(\d+(?:[.,]\d*)?Y)?[,]?(\d+(?:[.,]\d*)?M)?[,]?"
            r"(\d+(?:[.,]\d*)?D)?[,]?(T(?=\d)(?:(\d+(?:[.,]\d*)?H)?[,]?"
            r"(\d+(?:[.,]\d*)?M)?[,]?(\d+(?:[.,]\d*)?S)?)?)?)|(\d+(?:[.,]\d*)?W))$"
        )
    match = re.match(pattern, duration)
    if not match:
        return False

    years, months, days, time_designator, hours, minutes, seconds, weeks = (
        match.groups()
    )

    if time_designator and not any([hours, minutes, seconds]):
        return False

    components = [
        c
        for c in [years, months, weeks, days, hours, minutes, seconds]
        if c is not None
    ]

    decimal_found = False
    for i, component in enumerate(components):
        if "." in component or "," in component:
            if decimal_found or i != len(components) - 1:
                return False
            decimal_found = True

    return True


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


def _detect_time_precision(time_part: str) -> str:
    if "." in time_part:
        return "microsecond"

    colon_count = time_part.count(":")
    if colon_count >= 2:
        return "second"
    elif colon_count == 1:
        return "minute"
    else:
        return "hour"


def _detect_date_precision_simple(date_str: str) -> str:
    date_parts = [p for p in date_str.split("-") if p]
    if len(date_parts) >= 3:
        return "day"
    elif len(date_parts) == 2:
        return "month"
    elif len(date_parts) == 1:
        return "year"
    return None


@lru_cache(maxsize=1000)
def detect_date_precision(date_str: str) -> str:
    if not date_str or not isinstance(date_str, str):
        return None

    if "T" not in date_str and "--" not in date_str and "-:" not in date_str:
        return _detect_date_precision_simple(date_str)

    if "--" in date_str or "-:" in date_str:
        date_str = date_str.split("--")[0].split("-:")[0]
        if not date_str or date_str.endswith("-"):
            date_str = date_str.rstrip("-")

    if "T" in date_str:
        time_part = date_str.split("T")[1]
        if not time_part:
            return "day"
        time_part = time_part.split("+")[0].split("-")[-1].split("Z")[0]
        return _detect_time_precision(time_part)

    return _detect_date_precision_simple(date_str)


PRECISION_ORDER = {
    "year": 0,
    "month": 1,
    "day": 2,
    "hour": 3,
    "minute": 4,
    "second": 5,
    "microsecond": 6,
}

PRECISION_LEVELS = [
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "microsecond",
]


def get_common_precision(dt1: str, dt2: str) -> str:
    p1 = detect_date_precision(dt1)
    p2 = detect_date_precision(dt2)

    if not p1 or not p2:
        return None

    min_idx = min(PRECISION_ORDER[p1], PRECISION_ORDER[p2])
    return PRECISION_LEVELS[min_idx]


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
        return date.astimezone(utc)
    else:
        return utc.localize(date)


def is_complete_date(date_string: str) -> bool:
    try:
        datetime.fromisoformat(date_string)
    except Exception as e:
        try:
            datetime.fromisoformat(date_string.replace("Z", "+00:00"))
        except Exception as e:
            logger.error(
                f"Error with date parsing: {str(e)}, "
                f"traceback: {traceback.format_exc()}"
            )
            return False
        logger.error(
            f"Error with date parsing: {str(e)}, "
            f"traceback: {traceback.format_exc()}"
        )
        return True
    return True


def get_dict_key_val(dict_to_get: dict, key):
    return dict_to_get.get(key)


def is_in(value, values):
    if values is None:
        return False
    if value is None:
        return False
    if isinstance(value, (float)):
        if np.isnan(value):
            return False
    return value in values


def case_insensitive_is_in(value, values):
    return str(value).lower() in str(values).lower()


def compare_dates(component, target, comparator, operator):
    if not target or not comparator:
        return False

    if component == "auto":
        component = get_common_precision(target, comparator)

    return operator(
        get_date_component(component, target),
        get_date_component(component, comparator),
    )


def apply_regex(regex: str, val: str):
    result = re.findall(regex, val)
    if result:
        return result[0]
    else:
        return None


def flatten_list(data, items):
    for item in items:
        if isinstance(item, list):
            yield from flatten_list(data, item)
        elif item in data and isinstance(data[item].iloc[0], list):
            for val in data[item].iloc[0]:
                yield val
        else:
            yield item


vectorized_apply_regex = np.vectorize(apply_regex)
vectorized_is_complete_date = np.vectorize(is_complete_date)
vectorized_compare_dates = np.vectorize(compare_dates)
vectorized_is_valid = np.vectorize(is_valid_date)
vectorized_is_valid_duration = np.vectorize(is_valid_duration)
vectorized_get_dict_key = np.vectorize(get_dict_key_val)
vectorized_is_in = np.vectorize(is_in)
vectorized_case_insensitive_is_in = np.vectorize(case_insensitive_is_in)
vectorized_len = np.vectorize(len)
