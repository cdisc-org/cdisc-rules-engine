from datetime import datetime
import re
import numpy as np
from dateutil.parser import parse, isoparse
import pytz
from cdisc_rules_engine.services import logger
import traceback
from functools import lru_cache
from enum import IntEnum
import operator

# Date regex pattern for validation
date_regex = re.compile(
    r"^("
    r"(?P<year>-?[0-9]{4}|-)(-{1,2}(?P<month>1[0-2]|0[1-9]|-))?"
    r"(-{1,2}(?P<day>3[01]|0[1-9]|[12][0-9]|-))?"
    r"(T(?P<hour>2[0-3]|[01][0-9]|-)(:((?P<minute>[0-5][0-9]|-))"
    r"(:((?P<second>[0-5][0-9]|-))?(\.(?P<microsecond>[0-9]+))?)?)?"
    r"(?P<timezone>Z|[+-](2[0-3]|[01][0-9]):[0-5][0-9])?)?"
    r"(\/"
    r"(?P<interval_year>-?[0-9]{4}|-)(-{1,2}(?P<interval_month>1[0-2]|0[1-9]|-))?"
    r"(-{1,2}(?P<interval_day>3[01]|0[1-9]|[12][0-9]|-))?"
    r"(T(?P<interval_hour>2[0-3]|[01][0-9]|-)(:((?P<interval_minute>[0-5][0-9]|-))"
    r"(:((?P<interval_second>[0-5][0-9]|-))?(\.(?P<interval_microsecond>[0-9]+))?)?)?"
    r"(?P<interval_timezone>Z|[+-](2[0-3]|[01][0-9]):[0-5][0-9])?)?"
    r")?"
    r"|"
    r"-{4,8}T(?P<timeonly_hour>2[0-3]|[01][0-9]|-)(:((?P<timeonly_minute>[0-5][0-9]|-))"
    r"(:((?P<timeonly_second>[0-5][0-9]|-))?(\.(?P<timeonly_microsecond>[0-9]+))?)?)?"
    r"(?P<timeonly_timezone>Z|[+-](2[0-3]|[01][0-9]):[0-5][0-9])?"
    r")$"
)


class DatePrecision(IntEnum):
    year = 0
    month = 1
    day = 2
    hour = 3
    minute = 4
    second = 5
    microsecond = 6


def is_valid_date(date_string: str) -> bool:
    if date_string is None:
        return False
    try:
        isoparse(date_string)
    except Exception as e:
        uncertainty_substrings = ["/", "--", "-:"]
        if any([substr in date_string for substr in uncertainty_substrings]):
            # date_string contains uncertainty
            # will not parse with isoparse
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
    # Check if decimal is only in the smallest unit
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


def _empty_datetime_components() -> dict:
    return {
        "year": None,
        "month": None,
        "day": None,
        "hour": None,
        "minute": None,
        "second": None,
        "microsecond": None,
    }


def _extract_datetime_components(date_str: str) -> dict:
    """Extract datetime components using regex pattern matching."""
    components = _empty_datetime_components()
    if not date_str or not isinstance(date_str, str):
        return components

    match = date_regex.match(date_str)
    if not match:
        return components

    year = match.group("year") or match.group("interval_year")
    month = match.group("month") or match.group("interval_month")
    day = match.group("day") or match.group("interval_day")
    hour = (
        match.group("hour")
        or match.group("interval_hour")
        or match.group("timeonly_hour")
    )
    minute = (
        match.group("minute")
        or match.group("interval_minute")
        or match.group("timeonly_minute")
    )
    second = (
        match.group("second")
        or match.group("interval_second")
        or match.group("timeonly_second")
    )
    microsecond = (
        match.group("microsecond")
        or match.group("interval_microsecond")
        or match.group("timeonly_microsecond")
    )

    if year and year != "-":
        components["year"] = year
    if month and month != "-":
        components["month"] = month
    if day and day != "-":
        components["day"] = day
    if hour and hour != "-":
        components["hour"] = hour
    if minute and minute != "-":
        components["minute"] = minute
    if second and second != "-":
        components["second"] = second
    if microsecond:
        components["microsecond"] = microsecond

    return components


def _parse_datetime_string(date_str: str):
    if not date_str or not isinstance(date_str, str):
        return [], (None, None, None, None), False

    match = date_regex.match(date_str)
    if not match:
        return [], (None, None, None, None), False

    year = match.group("year") or match.group("interval_year")
    month = match.group("month") or match.group("interval_month")
    day = match.group("day") or match.group("interval_day")
    hour = (
        match.group("hour")
        or match.group("interval_hour")
        or match.group("timeonly_hour")
    )
    minute = (
        match.group("minute")
        or match.group("interval_minute")
        or match.group("timeonly_minute")
    )
    second = (
        match.group("second")
        or match.group("interval_second")
        or match.group("timeonly_second")
    )
    microsecond = (
        match.group("microsecond")
        or match.group("interval_microsecond")
        or match.group("timeonly_microsecond")
    )

    date_components = [
        year if year and year != "-" else "-",
        month if month and month != "-" else "-",
        day if day and day != "-" else "-",
    ]

    has_time = (
        hour is not None
        or minute is not None
        or second is not None
        or microsecond is not None
    )

    time_components = (
        hour if hour and hour != "-" else None,
        minute if minute and minute != "-" else None,
        second if second and second != "-" else None,
        microsecond if microsecond else None,
    )

    return date_components, time_components, has_time


@lru_cache(maxsize=1000)
def detect_datetime_precision(date_str: str) -> DatePrecision | None:
    if not _datestring_is_valid(date_str):
        return None

    date_components, time_components, has_time = _parse_datetime_string(date_str)

    if all(
        component == "-" or component is None or component == ""
        for component in date_components
    ):
        return None

    return _date_and_time_precision(date_components, time_components, has_time)


def _datestring_is_valid(date_str: str) -> bool:
    return bool(date_str and isinstance(date_str, str) and date_regex.match(date_str))


def _check_date_component_missing(component) -> bool:
    return component is None or component == "-" or component == ""


def _get_precision_before(precision: DatePrecision) -> DatePrecision | None:
    if precision == DatePrecision.year:
        return None
    prev_index = precision.value - 1
    if prev_index >= 0:
        return DatePrecision(prev_index)
    return None


def _check_date_precision(date_components) -> tuple:
    date_component_map = {
        DatePrecision.year: 0,
        DatePrecision.month: 1,
        DatePrecision.day: 2,
    }
    for precision in [DatePrecision.year, DatePrecision.month, DatePrecision.day]:
        index = date_component_map[precision]
        component = date_components[index] if index < len(date_components) else None
        if _check_date_component_missing(component):
            result = _get_precision_before(precision)
            return (True, result)
    return (False, None)


def _check_time_precision(time_components) -> DatePrecision:
    if not time_components:
        return DatePrecision.day
    hour, minute, second, microsecond = time_components
    if _check_date_component_missing(hour):
        return DatePrecision.day
    if _check_date_component_missing(minute):
        return DatePrecision.hour
    if second is None:
        return DatePrecision.minute
    if _check_date_component_missing(second):
        return DatePrecision.minute
    if microsecond is None:
        return DatePrecision.second
    if not microsecond or microsecond == "":
        return DatePrecision.second
    return DatePrecision.microsecond


def _date_and_time_precision(
    date_components, time_components, has_time
) -> DatePrecision | None:
    found_missing, date_result = _check_date_precision(date_components)
    if found_missing:
        return date_result
    if not has_time or not time_components:
        return DatePrecision.day
    return _check_time_precision(time_components)


def get_common_precision(dt1: str, dt2: str) -> DatePrecision | None:
    p1 = detect_datetime_precision(dt1)
    p2 = detect_datetime_precision(dt2)
    if p1 is None or p2 is None:
        return None
    min_idx = min(p1.value, p2.value)
    return DatePrecision(min_idx)


def get_date_component(component: str, date_string: str):
    try:
        precision = DatePrecision[component]
    except (KeyError, ValueError):
        return get_date(date_string)

    component_func_map = {
        DatePrecision.year: get_year,
        DatePrecision.month: get_month,
        DatePrecision.day: get_day,
        DatePrecision.hour: get_hour,
        DatePrecision.minute: get_minute,
        DatePrecision.microsecond: get_microsecond,
        DatePrecision.second: get_second,
    }
    component_function = component_func_map.get(precision)
    if component_function:
        return component_function(date_string)
    else:
        return get_date(date_string)


def _parse_uncertain_date(date_string: str) -> datetime | None:
    """Parse uncertain dates with missing components using regex groups."""
    components = _extract_datetime_components(date_string)

    year = int(components.get("year") or 1970)
    month = int(components.get("month") or 1)
    day = int(components.get("day") or 1)
    hour = int(components.get("hour") or 0)
    minute = int(components.get("minute") or 0)
    second = int(components.get("second") or 0)
    microsecond = int(components.get("microsecond") or 0)

    try:
        return datetime(year, month, day, hour, minute, second, microsecond)
    except (ValueError, TypeError):
        return None


def get_date(date_string: str):
    """
    Returns a utc timestamp for comparison
    """
    uncertainty_substrings = ["/", "--", "-:"]
    has_uncertainty = any([substr in date_string for substr in uncertainty_substrings])

    if has_uncertainty:
        uncertain_date = _parse_uncertain_date(date_string)
        if uncertain_date is not None:
            utc = pytz.UTC
            return utc.localize(uncertain_date)

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


def truncate_datetime_to_precision(date_string: str, precision: DatePrecision):
    dt = get_date(date_string)
    match precision:
        case DatePrecision.year:
            return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        case DatePrecision.month:
            return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        case DatePrecision.day:
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        case DatePrecision.hour:
            return dt.replace(minute=0, second=0, microsecond=0)
        case DatePrecision.minute:
            return dt.replace(second=0, microsecond=0)
        case DatePrecision.second:
            return dt.replace(microsecond=0)
        case DatePrecision.microsecond:
            return dt
        case _:
            return dt


def _dates_are_comparable(target: str, comparator: str) -> bool:
    if not target or not comparator:
        return False
    return is_valid_date(target) and is_valid_date(comparator)


def _has_explicit_component(component) -> bool:
    return component not in (None, "auto")


def _compare_with_component(component, target, comparator, operator_func):
    return operator_func(
        get_date_component(component, target),
        get_date_component(component, comparator),
    )


def _build_precision_context(target: str, comparator: str) -> dict:
    return {
        "target_precision": detect_datetime_precision(target),
        "comparator_precision": detect_datetime_precision(comparator),
        "precision": get_common_precision(target, comparator),
    }


def _truncate_by_precision(
    target: str, comparator: str, precision: DatePrecision | None
) -> tuple:
    if precision is None:
        return get_date(target), get_date(comparator)
    return (
        truncate_datetime_to_precision(target, precision),
        truncate_datetime_to_precision(comparator, precision),
    )


def _compare_with_inferred_precision(
    operator_func,
    target: str,
    comparator: str,
    truncated_target,
    truncated_comparator,
    context: dict,
):
    target_precision = context["target_precision"]
    comparator_precision = context["comparator_precision"]

    if operator_func is operator.eq:
        if target_precision != comparator_precision:
            return False
        return truncated_target == truncated_comparator

    if operator_func is operator.ne:
        if target_precision != comparator_precision:
            return True
        return truncated_target != truncated_comparator

    result = operator_func(truncated_target, truncated_comparator)

    if truncated_target == truncated_comparator:
        if target_precision and comparator_precision:
            if target_precision.value > comparator_precision.value:
                return operator_func(get_date(target), get_date(comparator))
        return result

    return result


def compare_dates(component, target, comparator, operator_func):
    if not _dates_are_comparable(target, comparator):
        return False

    if _has_explicit_component(component):
        return _compare_with_component(component, target, comparator, operator_func)

    context = _build_precision_context(target, comparator)
    precision = context["precision"]
    if precision is None:
        return False

    truncated_target, truncated_comparator = _truncate_by_precision(
        target, comparator, precision
    )

    if component == "auto":
        return operator_func(truncated_target, truncated_comparator)

    return _compare_with_inferred_precision(
        operator_func,
        target,
        comparator,
        truncated_target,
        truncated_comparator,
        context,
    )


def apply_regex(regex: str, val: str):
    result = re.findall(regex, val)
    if result:
        return result[0]
    else:
        return None


def apply_rounding(target_val, comparison_val):
    try:
        rounded_target = round(float(target_val)) if target_val is not None else None
    except (ValueError, TypeError):
        rounded_target = target_val
    try:
        rounded_comparison = (
            round(float(comparison_val)) if comparison_val is not None else None
        )
    except (ValueError, TypeError):
        rounded_comparison = comparison_val
    return rounded_target, rounded_comparison


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
