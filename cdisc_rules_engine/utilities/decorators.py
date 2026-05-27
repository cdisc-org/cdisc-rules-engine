from functools import wraps
from typing import Callable


def cached(cache_key: str):  # noqa: C901
    """
    Generic decorator for cached data.
    #All cached data should have a cache key of the format:
    {study_id}/{data_bundle_id}/{domain_name}/{arg}.../key.

    Note: It is expected that the instance has a cache_service property.
    """

    def format_cache_key(
        key: str,
        args=[],
        study_id=None,
        data_bundle_id=None,
        domain_name=None,
        name=None,
    ):
        """
        If a study_id and data_bundle_id are available,
        cache_key = {study_id}/{data_bundle_id}/key
        else the function just returns the provided cache key.
        """
        if name:
            key = f"{name}/" + key
        if domain_name:
            key = f"{domain_name}/" + key
        if data_bundle_id:
            key = f"{data_bundle_id}/" + key
        if study_id:
            key = f"{study_id}/" + key
        for arg in args:
            if isinstance(arg, str):
                key = f"{arg}/" + key
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
                instance.domain
                if hasattr(instance, "domain")
                else kwargs.get("domain_name")
            )
            name = instance.name if hasattr(instance, "name") else kwargs.get("name")
            if (
                hasattr(instance, "cache_service")
                and instance.cache_service is not None
            ):
                key = format_cache_key(
                    cache_key,
                    args,
                    study_id=study_id,
                    data_bundle_id=data_bundle_id,
                    domain_name=domain_name,
                    name=name,
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
