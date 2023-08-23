import itertools
import time
from functools import partial
from multiprocessing import Pool
from multiprocessing.managers import SyncManager
from typing import List, Iterable, Callable

from cdisc_rules_engine.config import config
from cdisc_rules_engine.enums.progress_parameter_options import ProgressParameterOptions
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
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
    get_cache_service,
    get_library_metadata_from_cache,
    get_rules,
    get_datasets,
)
from cdisc_rules_engine.services.reporting import BaseReport, ReportFactory
from cdisc_rules_engine.utilities.progress_displayers import get_progress_displayer
from warnings import simplefilter

simplefilter(
    action="ignore", category=FutureWarning
)  # Suppress warnings coming from numpy
"""
Sync manager used to manage instances of the cache between processes.
Cache types are registered to this manager, and only one instance of the
cache is created at startup and provided to each process.
"""


class CacheManager(SyncManager):
    pass


def validate_single_rule(
    cache,
    datasets,
    args: Validation_args,
    library_metadata: LibraryMetadataContainer,
    rule: dict = None,
):
    rule["conditions"] = ConditionCompositeFactory.get_condition_composite(
        rule["conditions"]
    )
    set_log_level(args)
    # call rule engine
    engine = RulesEngine(
        cache=cache,
        standard=args.standard,
        standard_version=args.version.replace(".", "-"),
        ct_packages=args.controlled_terminology_package,
        meddra_path=args.meddra,
        whodrug_path=args.whodrug,
        define_xml_path=args.define_xml_path,
        library_metadata=library_metadata,
    )
    results = []
    validated_domains = set()
    for dataset in datasets:
        # Check if the domain has been validated before
        # This addresses the case where a domain is split
        # and appears multiple times within the list of datasets
        if dataset["domain"] not in validated_domains:
            validated_domains.add(dataset["domain"])
            results.append(
                engine.validate_single_rule(
                    rule, dataset["full_path"], datasets, dataset["domain"]
                )
            )

    results = list(itertools.chain(*results))
    if args.progress == ProgressParameterOptions.VERBOSE_OUTPUT.value:
        engine_logger.log(f"{rule['core_id']} validation complete")
    return RuleValidationResult(rule, results)


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
    library_metadata: LibraryMetadataContainer = get_library_metadata_from_cache(args)
    # install dictionaries if needed
    fill_cache_with_dictionaries(shared_cache, args)
    rules = get_rules(args)
    data_service = DataServiceFactory(config, shared_cache).get_data_service()
    datasets = get_datasets(data_service, args.dataset_paths)
    engine_logger.info(f"Running {len(rules)} rules against {len(datasets)} datasets")
    start = time.time()
    results = []
    # run each rule in a separate process
    with Pool(args.pool_size) as pool:
        validation_results: Iterable[RuleValidationResult] = pool.imap_unordered(
            partial(
                validate_single_rule, shared_cache, datasets, args, library_metadata
            ),
            rules,
        )
        progress_handler: Callable = get_progress_displayer(args)
        results = progress_handler(rules, validation_results, results)

    # build all desired reports
    end = time.time()
    elapsed_time = end - start
    reporting_factory = ReportFactory(
        datasets, results, elapsed_time, args, data_service
    )
    reporting_services: List[BaseReport] = reporting_factory.get_report_services()
    for reporting_service in reporting_services:
        reporting_service.write_report(args.define_xml_path)
