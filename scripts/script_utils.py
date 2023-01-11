from cdisc_rules_engine.interfaces import CacheServiceInterface, DataServiceInterface
from cdisc_rules_engine.services.data_services import (
    DataServiceFactory,
)
from typing import List, Iterable
from cdisc_rules_engine.config import config
from cdisc_rules_engine.services import logger as engine_logger
import os
import pickle
from cdisc_rules_engine.models.dictionaries import DictionaryTypes
from cdisc_rules_engine.models.dictionaries.get_dictionary_terms import (
    extract_dictionary_terms,
)
from cdisc_rules_engine.utilities.utils import get_rules_cache_key


def fill_cache_with_provided_data(cache, args):
    cache_files = next(os.walk(args.cache), (None, None, []))[2]
    for file_name in cache_files:
        if "ct-" in file_name:
            ct_version = file_name.split(".")[0]
            if (
                args.controlled_terminology_package
                and ct_version in args.controlled_terminology_package
            ):
                # Only load ct package corresponding to the provided ct
                with open(f"{args.cache}/{file_name}", "rb") as f:
                    data = pickle.load(f)
                    cache.add(ct_version, data)
            else:
                continue
        with open(f"{args.cache}/{file_name}", "rb") as f:
            data = pickle.load(f)
            cache.add_all(data)
    return cache


def fill_cache_with_dictionaries(cache: CacheServiceInterface, args):
    """
    Extracts file contents from provided dictionaries files
    and saves to cache (inmemory or redis).
    """
    if not args.meddra and not args.whodrug:
        return

    data_service = DataServiceFactory(config, cache).get_data_service()

    dictionary_type_to_path_map: dict = {
        DictionaryTypes.MEDDRA: args.meddra,
        DictionaryTypes.WHODRUG: args.whodrug,
    }
    for dictionary_type, dictionary_path in dictionary_type_to_path_map.items():
        if not dictionary_path:
            continue
        terms = extract_dictionary_terms(data_service, dictionary_type, dictionary_path)
        cache.add(dictionary_path, terms)


def get_cache_service(manager):
    cache_service_type = config.getValue("CACHE_TYPE")
    if cache_service_type == "redis":
        return manager.RedisCacheService(
            config.getValue("REDIS_HOST_NAME"), config.getValue("REDIS_ACCESS_KEY")
        )
    else:
        return manager.InMemoryCacheService()


def get_rules(cache: CacheServiceInterface, args) -> List[dict]:
    if args.rules:
        keys = [
            get_rules_cache_key(args.standard, args.version.replace(".", "-"), rule)
            for rule in args.rules
        ]
        rules = cache.get_all(keys)
    else:
        engine_logger.warning(
            f"No rules specified. Running all rules for {args.standard}"
            + f" version {args.version}"
        )
        rules = cache.get_all_by_prefix(
            get_rules_cache_key(args.standard, args.version.replace(".", "-"))
        )
    return rules


def get_datasets(
    data_service: DataServiceInterface, dataset_paths: Iterable[str]
) -> List[dict]:
    datasets = []
    for dataset_path in dataset_paths:
        metadata = data_service.get_dataset_metadata(dataset_name=dataset_path)
        datasets.append(
            {
                "domain": metadata["dataset_name"].iloc[0],
                "filename": metadata["dataset_location"].iloc[0],
                "full_path": dataset_path,
            }
        )

    return datasets
