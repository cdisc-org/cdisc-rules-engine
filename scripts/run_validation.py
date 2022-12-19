import itertools
import time
from functools import partial
from multiprocessing import Pool
from multiprocessing.managers import SyncManager
from typing import List, Iterable, Callable

from cdisc_rules_engine.config import config
from cdisc_rules_engine.enums.progress_parameter_options import ProgressParameterOptions
from cdisc_rules_engine.models.rule_conditions import ConditionCompositeFactory
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.rules_engine import RulesEngine
from cdisc_rules_engine.services import logger as engine_logger
from cdisc_rules_engine.services.cache import (
    InMemoryCacheService,
    RedisCacheService,
)
from cdisc_rules_engine.services.data_services import (
    DataServiceFactory,
)
from scripts.script_utils import (
    fill_cache_with_dictionaries,
    fill_cache_with_provided_data,
    get_cache_service,
    get_rules,
    get_datasets,
)
from cdisc_rules_engine.services.reporting import BaseReport, ReportFactory
from cdisc_rules_engine.utilities.progress_displayers import get_progress_displayer

"""
Sync manager used to manage instances of the cache between processes.
Cache types are registered to this manager, and only one instance of the
cache is created at startup and provided to each process.
"""


class CacheManager(SyncManager):
    pass


def validate_single_rule(cache, datasets, args: Validation_args, rule: dict = None):
    rule["conditions"] = ConditionCompositeFactory.get_condition_composite(
        rule["conditions"]
    )
    set_log_level(args)
    # call rule engine
    engine = RulesEngine(
        cache=cache,
        standard=args.standard,
        standard_version=args.version.replace(".", "-"),
        ct_package=args.controlled_terminology_package,
        meddra_path=args.meddra,
        whodrug_path=args.whodrug,
    )
    results = [
        engine.validate_single_rule(
            rule, dataset["full_path"], datasets, dataset["domain"]
        )
        for dataset in datasets
    ]
    results = list(itertools.chain(*results))
    if args.progress == ProgressParameterOptions.VERBOSE_OUTPUT.value:
        engine_logger.log(f"{rule['core_id']} validation complete")
    return RuleValidationResult(rule, results)

def fill_cache_with_provided_data(cache, cache_path: str, args):
    cache_files = next(os.walk(cache_path), (None, None, []))[2]
    for file_name in cache_files:
        if "ct-" in file_name:
            ct_version = file_name.split(".")[0]
            if (
                args.controlled_terminology_package
                and ct_version in args.controlled_terminology_package
            ):
                # Only load ct package corresponding to the provided ct
                with open(f"{cache_path}/{file_name}", "rb") as f:
                    data = pickle.load(f)
                    cache.add(ct_version, data)
            else:
                continue
        with open(f"{cache_path}/{file_name}", "rb") as f:
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

def set_log_level(args):
    if args.log_level.lower() == "disabled":
        engine_logger.disabled = True
    else:
        engine_logger.setLevel(args.log_level.lower())
    if args.progress == ProgressParameterOptions.VERBOSE_OUTPUT.value:
        engine_logger.disabled = False
        engine_logger.setLevel("verbose")


def run_validation(args: Validation_args):
    set_log_level(args)
    # fill cache
    CacheManager.register("RedisCacheService", RedisCacheService)
    CacheManager.register("InMemoryCacheService", InMemoryCacheService)
    manager = CacheManager()
    manager.start()
    shared_cache = get_cache_service(manager)
    engine_logger.info(f"Populating cache, cache path: {args.cache}")
    shared_cache = fill_cache_with_provided_data(shared_cache, args)

    # install dictionaries if needed
    fill_cache_with_dictionaries(shared_cache, args)
    rules = get_rules(shared_cache, args)
    data_service = DataServiceFactory(config, shared_cache).get_data_service()
    datasets = get_datasets(data_service, args.dataset_paths)
    engine_logger.info(f"Running {len(rules)} rules against {len(datasets)} datasets")

    start = time.time()
    results = []
    # run each rule in a separate process
    with Pool(args.pool_size) as pool:
        validation_results: Iterable[RuleValidationResult] = pool.imap_unordered(
            partial(validate_single_rule, shared_cache, datasets, args),
            rules,
        )
        progress_handler: Callable = get_progress_displayer(args)
        results = progress_handler(rules, validation_results, results)

    # build all desired reports
    end = time.time()
    elapsed_time = end - start
    reporting_factory = ReportFactory(
        args.dataset_paths, results, elapsed_time, args, data_service
    )
    reporting_services: List[BaseReport] = reporting_factory.get_report_services()
    for reporting_service in reporting_services:
        reporting_service.write_report()
