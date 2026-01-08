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

    @property
    def default_value(self):
        default_values = {
            DatePrecision.year: 1970,
            DatePrecision.month: 1,
            DatePrecision.day: 1,
            DatePrecision.hour: 0,
            DatePrecision.minute: 0,
            DatePrecision.second: 0,
            DatePrecision.microsecond: 0,
        }
        return default_values[self]


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


def _empty_datetime_components():
    return {precision: None for precision in DatePrecision}


def _extract_datetime_components(date_str: str) -> dict:
    """Extract datetime components using regex pattern matching."""
    if not date_str or not isinstance(date_str, str):
        return _empty_datetime_components()
    match = date_regex.match(date_str)
    if not match:
        return _empty_datetime_components()

    matches = {
        DatePrecision.year: match.group("year") or match.group("interval_year"),
        DatePrecision.month: match.group("month") or match.group("interval_month"),
        DatePrecision.day: match.group("day") or match.group("interval_day"),
        DatePrecision.hour: (
            match.group("hour")
            or match.group("interval_hour")
            or match.group("timeonly_hour")
        ),
        DatePrecision.minute: (
            match.group("minute")
            or match.group("interval_minute")
            or match.group("timeonly_minute")
        ),
        DatePrecision.second: (
            match.group("second")
            or match.group("interval_second")
            or match.group("timeonly_second")
        ),
        DatePrecision.microsecond: (
            match.group("microsecond")
            or match.group("interval_microsecond")
            or match.group("timeonly_microsecond")
        ),
    }
    components = {
        precision: None if _check_date_component_missing(component) else component
        for precision, component in matches.items()
    }
    return components


@lru_cache(maxsize=1000)
def detect_datetime_precision(date_str: str) -> DatePrecision | None:
    if not _datestring_is_valid(date_str):
        return None
    components = _extract_datetime_components(date_str)
    if all(_check_date_component_missing(component) for component in components):
        return None
    return _date_and_time_precision(components)


def _datestring_is_valid(date_str: str) -> bool:
    return bool(date_str and isinstance(date_str, str) and date_regex.match(date_str))


def _check_date_component_missing(component) -> bool:
    return component is None or component == "-" or component == ""


def _get_precision_before(precision: DatePrecision) -> DatePrecision | None:
    prev_index = precision.value - 1
    return DatePrecision(prev_index) if prev_index >= 0 else None


def _date_and_time_precision(
    components: dict,
) -> DatePrecision | None:
    for precision in DatePrecision:
        component = components[precision] if precision in components else None
        if _check_date_component_missing(component):
            return _get_precision_before(precision)

    return DatePrecision.microsecond


def get_common_precision(dt1: str, dt2: str) -> DatePrecision | None:
    p1 = detect_datetime_precision(dt1)
    p2 = detect_datetime_precision(dt2)
    if p1 is None or p2 is None:
        return None
    min_idx = min(p1.value, p2.value)
    return DatePrecision(min_idx)


def get_date_component(component: str, date_string: str):
    date = get_date(date_string)
    try:
        return getattr(date, DatePrecision[component].name)
    except (KeyError, ValueError):
        return date


def _parse_uncertain_date(date_string: str) -> datetime | None:
    """Parse uncertain dates with missing components using regex groups."""
    components = _extract_datetime_components(date_string)
    component_ints = [
        int(components.get(precision) or precision.default_value)
        for precision in DatePrecision
    ]
    try:
        return datetime(*component_ints)
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

    date = parse(
        date_string,
        default=datetime(
            *[
                precision.default_value
                for precision in list(DatePrecision)[
                    DatePrecision.year : DatePrecision.day + 1
                ]
            ]
        ),
    )
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
    if precision is None:
        return dt
    replacements = {
        precision_component.name: precision_component.default_value
        for precision_component in list(DatePrecision)[precision.value + 1 :]
    }
    return dt.replace(**replacements)


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
