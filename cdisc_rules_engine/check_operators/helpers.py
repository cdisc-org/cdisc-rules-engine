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
    r"(-?[0-9]{4}|-)(-{1,2}(1[0-2]|0[1-9]|-))?(-{1,2}(3[01]|0[1-9]|[12][0-9]|-))?"
    r"(T(2[0-3]|[01][0-9]|-)(:(([0-5][0-9]|-))(:(([0-5][0-9]|-))?(\.[0-9]+)?)?)?"
    r"(Z|[+-](2[0-3]|[01][0-9]):[0-5][0-9])?)?"
    r"(\/"
    r"(-?[0-9]{4}|-)(-{1,2}(1[0-2]|0[1-9]|-))?(-{1,2}(3[01]|0[1-9]|[12][0-9]|-))?"
    r"(T(2[0-3]|[01][0-9]|-)(:(([0-5][0-9]|-))(:(([0-5][0-9]|-))?(\.[0-9]+)?)?)?"
    r"(Z|[+-](2[0-3]|[01][0-9]):[0-5][0-9])?)?"
    r")?"
    r"|"
    r"-{4,8}T(2[0-3]|[01][0-9]|-)(:(([0-5][0-9]|-))(:(([0-5][0-9]|-))?(\.[0-9]+)?)?)?"
    r"(Z|[+-](2[0-3]|[01][0-9]):[0-5][0-9])?"
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

    @classmethod
    def get_name_by_index(cls, index: int) -> str:
        return list(cls.__members__.keys())[index]

    @classmethod
    def names(cls) -> list:
        return list(cls.__members__.keys())


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


def _assign_date_components(components: dict, date_parts: list) -> None:
    if not date_parts:
        return
    keys = ("year", "month", "day")
    for index, key in enumerate(keys):
        if len(date_parts) > index and date_parts[index] not in (None, "-"):
            components[key] = date_parts[index]


def _assign_time_components(components: dict, time_part: str) -> None:
    if not time_part:
        return
    tokens = time_part.split(":", 2)
    if tokens and tokens[0] not in ("", "-"):
        components["hour"] = tokens[0]
    if len(tokens) > 1 and tokens[1] not in ("", "-"):
        components["minute"] = tokens[1]
    if len(tokens) == 3 and tokens[2]:
        second_part = tokens[2]
        if "." in second_part:
            second_val, micro_val = second_part.split(".", 1)
            if second_val not in ("", "-"):
                components["second"] = second_val
            if micro_val:
                components["microsecond"] = micro_val
        elif second_part not in ("", "-"):
            components["second"] = second_part


def _extract_datetime_components(date_str: str) -> dict:
    """Extract datetime components using regex pattern matching."""
    components = _empty_datetime_components()
    if not date_str or not isinstance(date_str, str):
        return components
    if not date_regex.match(date_str):
        return components
    date_parts, time_part, has_time = _parse_datetime_string(date_str)
    _assign_date_components(components, date_parts)
    if has_time:
        _assign_time_components(components, time_part)
    return components


def _parse_datetime_string(date_str: str):
    if not date_str or not isinstance(date_str, str):
        return [], "", False

    has_time = "T" in date_str

    if has_time:
        date_part, time_part = date_str.split("T", 1)
        time_part = re.sub(r"(Z|[+\-]\d{2}:\d{2})$", "", time_part)
    else:
        date_part = date_str
        time_part = ""

    if not date_part or all(c == "-" for c in date_part):
        return ["-", "-", "-"], time_part, has_time

    segments = date_part.split("-")
    components = []
    i = 0

    while i < len(segments) and len(components) < 3:
        segment = segments[i]

        if segment:
            components.append(segment)
            i += 1
        else:
            empty_start = i
            while i < len(segments) and not segments[i]:
                i += 1

            empty_count = i - empty_start
            if empty_count >= 2:
                components.append("-")

    while len(components) < 3:
        components.append("-")

    if len(components) > 3:
        components = components[:3]

    return components, time_part, has_time


def _check_date_component(date_components, index, precision_name):
    if len(date_components) <= index:
        if precision_name == "year":
            return None
        precision_names = DatePrecision.names()
        prev_index = precision_names.index(precision_name) - 1
        return precision_names[prev_index] if prev_index >= 0 else None

    component = date_components[index]

    if not component or component == "-":
        has_later_component = False
        for later_idx in range(index + 1, min(3, len(date_components))):
            if later_idx < len(date_components):
                later_comp = date_components[later_idx]
                if later_comp and later_comp != "-":
                    has_later_component = True
                    break

        if has_later_component:
            return None

        if precision_name == "year":
            return None
        precision_names = DatePrecision.names()
        prev_index = precision_names.index(precision_name) - 1
        return precision_names[prev_index] if prev_index >= 0 else None

    return None


def _check_time_component(time_part, has_time, component_index):
    if not has_time or not time_part:
        return "day"
    time_components = time_part.split(":")
    if len(time_components) <= component_index:
        precision_names = DatePrecision.names()
        prev_index = precision_names.index("hour") + component_index - 1
        return precision_names[prev_index] if prev_index >= 0 else "day"
    component = time_components[component_index]
    if not component or component == "-":
        precision_names = DatePrecision.names()
        prev_index = precision_names.index("hour") + component_index - 1
        return precision_names[prev_index] if prev_index >= 0 else "day"
    return None


def _check_second_component(time_part, has_time):
    if not has_time or not time_part:
        return "day"
    time_components = time_part.split(":")
    if len(time_components) <= 2:
        return "minute"
    second_part = time_components[2]
    second = second_part.split(".", 1)[0] if "." in second_part else second_part
    if not second or second == "-":
        return "minute"
    return None


def _check_microsecond_component(time_part, has_time):
    if not has_time or not time_part:
        return "day"
    time_components = time_part.split(":")
    if len(time_components) <= 2:
        return "minute"
    second_part = time_components[2]
    if "." not in second_part:
        return "second"
    microsecond_part = second_part.split(".", 1)[1]
    if not microsecond_part:
        return "second"
    return None


@lru_cache(maxsize=1000)
def detect_datetime_precision(date_str: str) -> str:
    if not _datestring_is_valid(date_str):
        return None

    date_components, time_part, has_time = _parse_datetime_string(date_str)

    if _is_time_only_precision(date_components, has_time):
        return _time_only_precision(time_part)

    return _date_and_time_precision(date_components, time_part, has_time)


def _datestring_is_valid(date_str: str) -> bool:
    return bool(date_str and isinstance(date_str, str) and date_regex.match(date_str))


def _is_time_only_precision(date_components: list, has_time: bool) -> bool:
    return has_time and all(component == "-" for component in date_components)


def _time_only_precision(time_part: str) -> str:
    if not time_part:
        return None

    time_components = time_part.split(":")
    if not time_components or not time_components[0] or time_components[0] == "-":
        return None
    if len(time_components) <= 1 or not time_components[1] or time_components[1] == "-":
        return "hour"
    if len(time_components) <= 2:
        return "minute"

    return _precision_from_second_component(time_components[2])


def _precision_from_second_component(second_part: str) -> str:
    if "." in second_part:
        second, microsecond = second_part.split(".", 1)
        if not second or second == "-":
            return "minute"
        return "second" if not microsecond else "microsecond"

    if not second_part or second_part == "-":
        return "minute"
    return "second"


def _date_and_time_precision(date_components, time_part, has_time) -> str:
    precision_checks = _precision_check_functions(date_components, time_part, has_time)

    for index, precision_name in enumerate(DatePrecision.names()):
        result = precision_checks[precision_name](index, precision_name)
        if result is not None:
            return result

    return "microsecond"


def _precision_check_functions(date_components, time_part, has_time):
    return {
        "year": lambda i, name: _check_date_component(date_components, i, name),
        "month": lambda i, name: _check_date_component(date_components, i, name),
        "day": lambda i, name: _check_date_component(date_components, i, name),
        "hour": lambda i, name: _check_time_component(time_part, has_time, 0),
        "minute": lambda i, name: _check_time_component(time_part, has_time, 1),
        "second": lambda i, name: _check_second_component(time_part, has_time),
        "microsecond": lambda i, name: _check_microsecond_component(
            time_part, has_time
        ),
    }


def get_common_precision(dt1: str, dt2: str) -> str:
    p1 = detect_datetime_precision(dt1)
    p2 = detect_datetime_precision(dt2)
    if not p1 or not p2:
        return None
    min_idx = min(DatePrecision[p1].value, DatePrecision[p2].value)
    return DatePrecision.get_name_by_index(min_idx)


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


def truncate_datetime_to_precision(date_string: str, precision: str):
    dt = get_date(date_string)
    if precision == "year":
        return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    if precision == "month":
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if precision == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if precision == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    if precision == "minute":
        return dt.replace(second=0, microsecond=0)
    if precision == "second":
        return dt.replace(microsecond=0)
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


def _truncate_by_precision(target: str, comparator: str, precision: str) -> tuple:
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
            target_value = DatePrecision[target_precision].value
            comparator_value = DatePrecision[comparator_precision].value
            if target_value > comparator_value:
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
