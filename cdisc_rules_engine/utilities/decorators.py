from functools import wraps
from typing import Callable, List

from requests import Response

from cdisc_rules_engine.exceptions.custom_exceptions import NumberOfAttemptsExceeded


def retry_request(retries: int, status_code_ranges: List[int] = None):
    """
    Decorator used to retry a request if
    any desired status was returned.
    Wrapped function has to return requests.Response object.
    """
    if status_code_ranges is None:
        status_code_ranges = [500]

    def decorator(func: Callable):
        @wraps(func)
        def inner(*args, **kwargs):
            for retry in range(retries):
                response: Response = func(*args, **kwargs)
                request_should_be_retried: bool = any(
                    response.status_code >= code_range
                    for code_range in status_code_ranges
                )
                # no need for else. If should be retied -> loop will go to the next iteration
                if not request_should_be_retried:
                    return response
            raise NumberOfAttemptsExceeded(
                f"Cannot perform request after {retries} retries. "
                f"Function - {func.__name__}. Args - {args}. Kwargs - {kwargs}."
            )

        return inner

    return decorator


def cached(cache_key: str):
    """
    Generic decorator for cached data. All cached data should have a cache key of the format:
    {study_id}/{data_bundle_id}/{domain_name}/key.

    Note: It is expected that the instance has a cache_service property.
    """

    def format_cache_key(
        key: str, study_id=None, data_bundle_id=None, domain_name=None
    ):
        """
        If a study_id and data_bundle_id are available, cache_key = {study_id}/{data_bundle_id}/key
        else the function just returns the provided cache key.
        """
        if domain_name:
            key = f"{domain_name}/" + key
        if data_bundle_id:
            key = f"{data_bundle_id}/" + key
        if study_id:
            key = f"{study_id}/" + key
        return key

    def decorator(func: Callable):
        @wraps(func)
        def inner(*args, **kwargs):
            instance = args[0]
            data_bundle_id = (
                instance.data_bundle_id
                if hasattr(instance, "data_bundle_id")
                else kwargs.get("data_bundle_id")
            )
            study_id = (
                instance.study_id
                if hasattr(instance, "study_id")
                else kwargs.get("study_id")
            )
            domain_name = (
                instance.domain_name
                if hasattr(instance, "domain_name")
                else kwargs.get("domain_name")
            )
            if (
                hasattr(instance, "cache_service")
                and instance.cache_service is not None
            ):
                key = format_cache_key(
                    cache_key,
                    study_id=study_id,
                    data_bundle_id=data_bundle_id,
                    domain_name=domain_name,
                )
                cached_data = instance.cache_service.get(key)
                if cached_data is not None:
                    return cached_data
                else:
                    data = func(*args, **kwargs)
                    instance.cache_service.add(key, data)
                    return data
            else:
                return func(*args, **kwargs)

        return inner

    return decorator
