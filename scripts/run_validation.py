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
from cdisc_rules_engine.models.dataset import PandasDataset
from scripts.script_utils import (
    fill_cache_with_dictionaries,
    get_cache_service,
    get_library_metadata_from_cache,
    get_rules,
    get_datasets,
    get_max_dataset_size,
)
from cdisc_rules_engine.services.reporting import BaseReport, ReportFactory
from cdisc_rules_engine.utilities.progress_displayers import get_progress_displayer
from warnings import simplefilter
import os

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
    max_dataset_size = max(datasets, key=lambda x: x["size"])["size"]
    # call rule engine
    engine = RulesEngine(
        cache=cache,
        standard=args.standard,
        standard_version=args.version.replace(".", "-"),
        standard_substandard=args.substandard,
        external_dictionaries=args.external_dictionaries,
        ct_packages=args.controlled_terminology_package,
        define_xml_path=args.define_xml_path,
        library_metadata=library_metadata,
        max_dataset_size=max_dataset_size,
        dataset_paths=args.dataset_paths,
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


def initialize_logger(disabled, log_level):
    if disabled:
        engine_logger.disabled = True
    else:
        engine_logger.disabled = False
        engine_logger.setLevel(log_level)


def run_validation(args: Validation_args):
    set_log_level(args)
    # fill cache
    CacheManager.register("RedisCacheService", RedisCacheService)
    CacheManager.register("InMemoryCacheService", InMemoryCacheService)
    manager = CacheManager()
    manager.start()
    shared_cache = get_cache_service(manager)
    engine_logger.info(f"Populating cache, cache path: {args.cache}")
    rules = get_rules(args)
    library_metadata: LibraryMetadataContainer = get_library_metadata_from_cache(args)
    # install dictionaries if needed
    dictionary_versions = fill_cache_with_dictionaries(shared_cache, args)
    max_dataset_size = get_max_dataset_size(args.dataset_paths)
    standard = args.standard
    standard_version = args.version.replace(".", "-")
    standard_substandard = args.substandard
    data_service = DataServiceFactory(
        config,
        shared_cache,
        max_dataset_size=max_dataset_size,
        standard=standard,
        standard_version=standard_version,
        standard_substandard=standard_substandard,
        library_metadata=library_metadata,
    ).get_data_service(args.dataset_paths)
    large_dataset_validation: bool = (
        data_service.dataset_implementation != PandasDataset
    )
    datasets = get_datasets(data_service, args.dataset_paths)
    created_files = []
    if large_dataset_validation and data_service.standard != "usdm":
        # convert all files to parquet temp files
        engine_logger.warning(
            "Large datasets must use parquet format, converting all datasets to parquet"
        )
        for dataset in datasets:
            file_path = dataset.get("full_path")
            if file_path.endswith(".parquet"):
                continue
            num_rows, new_file = data_service.to_parquet(file_path)
            created_files.append(new_file)
            dataset["full_path"] = new_file
            dataset["length"] = num_rows
            dataset["original_path"] = file_path
    engine_logger.info(f"Running {len(rules)} rules against {len(datasets)} datasets")
    start = time.time()
    results = []
    # instantiate logger in each child process to maintain log level
    initializer = partial(
        initialize_logger, engine_logger.disabled, engine_logger._logger.level
    )
    # run each rule in a separate process
    with Pool(args.pool_size, initializer=initializer) as pool:
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
        reporting_service.write_report(
            define_xml_path=args.define_xml_path,
            dictionary_versions=dictionary_versions,
        )
    print(f"Output: {args.output}")
    engine_logger.info("Cleaning up intermediate files")
    for file in created_files:
        engine_logger.info(f"Deleting file {file}")
        os.remove(file)
