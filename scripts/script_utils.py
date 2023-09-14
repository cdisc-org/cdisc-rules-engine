from cdisc_rules_engine.interfaces import CacheServiceInterface, DataServiceInterface
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
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
from cdisc_rules_engine.utilities.utils import (
    get_rules_cache_key,
    get_standard_details_cache_key,
    get_model_details_cache_key_from_ig,
    get_library_variables_metadata_cache_key,
    get_standard_codelist_cache_key,
)


def get_library_metadata_from_cache(args) -> LibraryMetadataContainer:
    standards_file = os.path.join(args.cache, "standards_details.pkl")
    models_file = os.path.join(args.cache, "standards_models.pkl")
    variables_codelist_file = os.path.join(args.cache, "variable_codelist_maps.pkl")
    variables_metadata_file = os.path.join(args.cache, "variables_metadata.pkl")

    standard_details_cache_key = get_standard_details_cache_key(
        args.standard, args.version.replace(".", "-")
    )
    with open(standards_file, "rb") as f:
        data = pickle.load(f)
        standard_metadata = data.get(standard_details_cache_key, {})

    model_cache_key = get_model_details_cache_key_from_ig(standard_metadata)
    with open(models_file, "rb") as f:
        data = pickle.load(f)
        model_details = data.get(model_cache_key, {})

    with open(variables_codelist_file, "rb") as f:
        data = pickle.load(f)
        cache_key = get_standard_codelist_cache_key(args.standard, args.version)
        variable_codelist_maps = data.get(cache_key)

    with open(variables_metadata_file, "rb") as f:
        data = pickle.load(f)
        cache_key = get_library_variables_metadata_cache_key(
            args.standard, args.version
        )
        variables_metadata = data.get(cache_key)

    ct_package_data = {}
    cache_files = next(os.walk(args.cache), (None, None, []))[2]
    ct_files = [file_name for file_name in cache_files if "ct-" in file_name]
    published_ct_packages = set()
    for file_name in ct_files:
        ct_version = file_name.split(".")[0]
        published_ct_packages.add(ct_version)
        if (
            args.controlled_terminology_package
            and ct_version in args.controlled_terminology_package
        ):
            # Only load ct package corresponding to the provided ct
            with open(os.path.join(args.cache, file_name), "rb") as f:
                data = pickle.load(f)
                ct_package_data[ct_version] = data

    return LibraryMetadataContainer(
        standard_metadata=standard_metadata,
        model_metadata=model_details,
        variable_codelist_map=variable_codelist_maps,
        variables_metadata=variables_metadata,
        ct_package_metadata=ct_package_data,
        published_ct_packages=published_ct_packages,
    )


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


def get_rules(args) -> List[dict]:
    core_ids = set()
    rules_file = os.path.join(args.cache, "rules.pkl")
    rules = []
    if args.rules:
        keys = [
            get_rules_cache_key(args.standard, args.version.replace(".", "-"), rule)
            for rule in args.rules
        ]
        with open(rules_file, "rb") as f:
            rules_data = pickle.load(f)
            rules = [rules_data.get(key) for key in keys]
    else:
        engine_logger.warning(
            f"No rules specified. Running all rules for {args.standard}"
            + f" version {args.version}"
        )
        with open(rules_file, "rb") as f:
            rules_data = pickle.load(f)
            for key, rule in rules_data.items():
                core_id = rule.get("core_id")
                rule_identifier = get_rules_cache_key(
                    args.standard, args.version.replace(".", "-"), core_id
                )
                if core_id not in core_ids and key == rule_identifier:
                    rules.append(rule)
                    core_ids.add(rule.get("core_id"))
    return rules


def get_datasets(
    data_service: DataServiceInterface, dataset_paths: Iterable[str]
) -> List[dict]:
    datasets = []
    for dataset_path in dataset_paths:
        metadata = data_service.get_raw_dataset_metadata(dataset_name=dataset_path)
        datasets.append(
            {
                "domain": metadata.domain_name,
                "filename": metadata.filename,
                "full_path": dataset_path,
                "length": metadata.records,
                "label": metadata.label,
                "size": metadata.size,
                "modification_date": metadata.modification_date,
            }
        )

    return datasets
