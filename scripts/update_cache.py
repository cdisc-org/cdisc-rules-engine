import asyncio
from functools import partial
from typing import List, Optional, Iterable
from engine.enums.library_endpoints import LibraryEndpoints
from engine.services.cache.in_memory_cache_service import InMemoryCacheService
from engine.services.cache.cache_service_interface import CacheServiceInterface
import pickle

from cdisc_library_client.custom_exceptions import (
    ResourceNotFoundException as LibraryResourceNotFoundException,
)

from engine.services.cdisc_library_service import CDISCLibraryService
from engine.utilities.utils import (
    get_library_variables_metadata_cache_key,
    get_standard_details_cache_key,
)
from datetime import datetime, timedelta


async def load_cache_data(api_key):
    """
    This function populates an in memory cache with all data necessary for running
    rules against local data. Including
    * rules
    * library metadata
    * codelist metadata
    """
    # send request to get all rules
    cache_service_obj = InMemoryCacheService()
    library_service = CDISCLibraryService(api_key, cache_service_obj)
    library_service.cache_library_json(LibraryEndpoints.PRODUCTS.value)
    library_service.cache_library_json(LibraryEndpoints.RULES.value)

    rules_lists: List[dict] = await get_rules_from_cdisc_library(library_service)
    for rules in rules_lists:
        cache_service_obj.add_batch(
            rules.get("rules", []), "core_id", prefix=rules.get("key_prefix")
        )

    # save codelists to cache as a map of codelist to terms
    codelist_term_maps = await get_codelist_term_maps(library_service)
    cache_service_obj.add_batch(codelist_term_maps, "package")

    # save standard codelists to cache as a map of variable to allowed_values
    standards = library_service.get_all_tabulation_ig_standards()
    standards.extend(library_service.get_all_collection_ig_standards())
    standards.extend(library_service.get_all_analysis_ig_standards())

    variable_codelist_maps = await get_variable_codelist_maps(
        library_service, standards
    )
    cache_service_obj.add_batch(variable_codelist_maps, "name")

    # save details of all standards to cache
    standards_details: List[dict] = await async_get_details_of_all_standards(
        library_service, standards
    )
    cache_service_obj.add_batch(standards_details, "cache_key", pop_cache_key=True)

    # save variables metadata to cache
    variables_metadata: Iterable[dict] = await get_variables_metadata(
        library_service, standards
    )
    cache_service_obj.add_batch(variables_metadata, "cache_key", pop_cache_key=True)
    return cache_service_obj


async def get_rules_from_cdisc_library(
    library_service: CDISCLibraryService,
) -> List[List[dict]]:
    """
    Requests rules from CDISC Library.
    """
    catalogs = library_service.get_all_rule_catalogs()
    coroutines = [
        async_get_rules_by_catalog(library_service, catalog.get("href"))
        for catalog in catalogs
    ]
    rules = await asyncio.gather(*coroutines)
    return rules


async def async_get_rules_by_catalog(
    library_service: CDISCLibraryService, catalog_link: str
) -> List[dict]:
    loop = asyncio.get_event_loop()
    standard = catalog_link.split("/")[-2]
    standard_version = catalog_link.split("/")[-1]
    rules: dict = await loop.run_in_executor(
        None, library_service.get_rules_by_catalog, standard, standard_version
    )
    return rules


async def get_codelist_term_maps(library_service: CDISCLibraryService) -> List[dict]:
    """
    For each CT package in CDISC library, generate a map of codelist to codelist term. Ex:
    {
        "package": "sdtmct-2021-12-17"
        "C123": {
            "extensible": True,
            "allowed_terms": ["TEST", "HOUR"]
        }
    }
    """
    packages = library_service.get_all_ct_packages()
    coroutines = [
        async_get_codelist_terms_map(
            library_service, package.get("href", "").split("/")[-1]
        )
        for package in packages
    ]
    codelist_term_maps = await asyncio.gather(*coroutines)
    return codelist_term_maps


async def async_get_codelist_terms_map(
    library_service: CDISCLibraryService, package_version: str
) -> dict:
    loop = asyncio.get_event_loop()
    terms_map: dict = await loop.run_in_executor(
        None, library_service.get_codelist_terms_map, package_version
    )
    return terms_map


async def get_variable_codelist_maps(
    library_service: CDISCLibraryService, standards: List[dict]
) -> List[dict]:
    coroutines = [
        async_get_variable_codelist_map(
            library_service,
            standard.get("href", "").split("/")[-2],
            standard.get("href", "").split("/")[-1],
        )
        for standard in standards
    ]
    variable_codelist_maps = await asyncio.gather(*coroutines)
    return variable_codelist_maps


async def async_get_variable_codelist_map(
    library_service: CDISCLibraryService, standard_type: str, standard_version: str
) -> dict:
    loop = asyncio.get_event_loop()
    variables_map: dict = await loop.run_in_executor(
        None,
        library_service.get_variable_codelists_map,
        standard_type,
        standard_version,
    )
    return variables_map


async def async_get_details_of_all_standards(
    library_service: CDISCLibraryService, standards: List[dict]
) -> List[dict]:
    """
    Gets details for each given standard.
    """
    coroutines = [
        async_get_standard_details(
            library_service,
            standard.get("href", "").split("/")[-2],
            standard.get("href", "").split("/")[-1],
        )
        for standard in standards
    ]
    return await asyncio.gather(*coroutines)


async def async_get_standard_details(
    library_service: CDISCLibraryService, standard_type: str, standard_version: str
) -> dict:
    """
    Gets details of a given standard.
    """
    loop = asyncio.get_event_loop()
    standard_details: dict = await loop.run_in_executor(
        None,
        library_service.get_standard_details,
        standard_type,
        standard_version,
    )
    standard_details["cache_key"] = get_standard_details_cache_key(
        standard_type, standard_version
    )
    return standard_details


async def get_variables_metadata(
    library_service: CDISCLibraryService, standards: List[dict]
) -> Iterable[dict]:
    """
    Returns a list of dicts of variables metadata for each standard.
    """
    coroutines = [
        async_get_variables_metadata(
            library_service,
            standard.get("href", "").split("/")[-2],
            standard.get("href", "").split("/")[-1],
        )
        for standard in standards
    ]
    metadata = await asyncio.gather(*coroutines)
    return filter(lambda item: item is not None, metadata)


async def async_get_variables_metadata(
    library_service: CDISCLibraryService, standard_type: str, standard_version: str
) -> Optional[dict]:
    """
    Returns variables metadata for a given standard.
    """
    loop = asyncio.get_event_loop()
    try:
        variables_metadata: dict = await loop.run_in_executor(
            None,
            partial(
                library_service.get_variables_details,
                standard_type,
                standard_version,
            ),
        )
    except LibraryResourceNotFoundException:
        return None
    return {
        "cache_key": get_library_variables_metadata_cache_key(
            standard_type, standard_version
        ),
        **variables_metadata,
    }


def save_rules_locally(cache: CacheServiceInterface, cache_path: str):
    """
    Store cached rules in rules.pkl in cache path directory
    """
    rules_data = cache.filter_cache("rules")
    with open(f"{cache_path}/rules.pkl", "wb") as f:
        pickle.dump(rules_data, f)


def save_ct_packages_locally(cache: CacheServiceInterface, cache_path: str):
    """
    Store cached ct pacakage metadata in codelist_term_maps.pkl in cache path directory
    """
    cts = cache.get_by_regex(".*ct.*")
    with open(f"{cache_path}/codelist_term_maps.pkl", "wb") as f:
        pickle.dump(cts, f)


def save_variable_codelist_maps_locally(cache: CacheServiceInterface, cache_path: str):
    """
    Store cached variable codelist metadata in variable_codelist_maps.pkl in cache path directory
    """
    variable_codelist_maps = cache.get_by_regex(".*codelists.*")
    with open(f"{cache_path}/variable_codelist_maps.pkl", "wb") as f:
        pickle.dump(variable_codelist_maps, f)


def save_standards_metadata_locally(cache: CacheServiceInterface, cache_path: str):
    """
    Store cached standards metadata in standards_details.pkl in cache path directory
    """
    standards = cache.filter_cache("standards")
    with open(f"{cache_path}/standards_details.pkl", "wb") as f:
        pickle.dump(standards, f)


def save_variables_metadata_locally(cache: CacheServiceInterface, cache_path: str):
    """
    Store cached variables metadata in variables_metadata.pkl in cache path directory
    """
    variables_metadata = cache.filter_cache("library_variables_metadata")
    with open(f"{cache_path}/variables_metadata.pkl", "wb") as f:
        pickle.dump(variables_metadata, f)
