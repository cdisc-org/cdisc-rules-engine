import itertools
import time
from functools import partial
from multiprocessing import Pool
from multiprocessing.managers import SyncManager
from typing import List, Iterable, Callable

from cdisc_rules_engine.config import config
from cdisc_rules_engine.config.config import ConfigService
from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset
from cdisc_rules_engine.enums.progress_parameter_options import ProgressParameterOptions
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.models.rule import Rule
from cdisc_rules_engine.models.rule_conditions import ConditionCompositeFactory
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.services import logger as engine_logger
from cdisc_rules_engine.services.cache import (
    InMemoryCacheService,
    RedisCacheService,
)
from cdisc_rules_engine.services.data_services import (
    DataServiceFactory,
)
from cdisc_rules_engine.models.dataset import PandasDataset
from cdisc_rules_engine.services.data_services.dummy_data_service import (
    DummyDataService,
)
from cdisc_rules_engine.sql_rules_engine import SQLRulesEngine
from cdisc_rules_engine.utilities.utils import (
    get_library_variables_metadata_cache_key,
    get_model_details_cache_key_from_ig,
    get_standard_details_cache_key,
    get_variable_codelist_map_cache_key,
)
from scripts.script_utils import (
    fill_cache_with_dictionaries,
    get_cache_service,
    get_library_metadata_from_cache,
    get_rules,
    get_max_dataset_size,
)
from cdisc_rules_engine.services.reporting import BaseReport, ReportFactory
from cdisc_rules_engine.utilities.progress_displayers import get_progress_displayer
from warnings import simplefilter
import os
from cdisc_rules_engine.constants.cache_constants import PUBLISHED_CT_PACKAGES

simplefilter(action="ignore", category=FutureWarning)  # Suppress warnings coming from numpy
"""
Sync manager used to manage instances of the cache between processes.
Cache types are registered to this manager, and only one instance of the
cache is created at startup and provided to each process.
"""


class CacheManager(SyncManager):
    pass


def sql_validate_single_rule(
    cache,
    datasets: Iterable[SDTMDatasetMetadata],
    args: Validation_args,
    library_metadata: LibraryMetadataContainer,
    rule: dict = None,
):
    rule["conditions"] = ConditionCompositeFactory.get_condition_composite(rule["conditions"])
    max_dataset_size = max(datasets, key=lambda x: x.file_size).file_size
    # call rule engine
    engine = SQLRulesEngine(
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
        validate_xml=args.validate_xml,
    )
    results = engine.sql_validate_single_rule(rule, datasets)
    results = list(itertools.chain(*results.values()))
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


def sql_run_validation(args: Validation_args):
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
    # install dictionaries if needed
    dictionary_versions = fill_cache_with_dictionaries(shared_cache, args, data_service)
    large_dataset_validation: bool = data_service.dataset_implementation != PandasDataset
    datasets = data_service.get_datasets()
    created_files = []
    if large_dataset_validation and data_service.standard != "usdm":
        # convert all files to parquet temp files
        engine_logger.warning("Large datasets must use parquet format, converting all datasets to parquet")
        for dataset in datasets:
            file_path = dataset.full_path
            if file_path.endswith(".parquet"):
                continue
            num_rows, new_file = data_service.to_parquet(file_path)
            created_files.append(new_file)
            dataset.full_path = new_file
            dataset.record_count = num_rows
            dataset.original_path = file_path
    engine_logger.info(f"Running {len(rules)} rules against {len(datasets)} datasets")
    start = time.time()
    results = []
    # instantiate logger in each child process to maintain log level
    initializer = partial(initialize_logger, engine_logger.disabled, engine_logger._logger.level)
    # run each rule in a separate process
    with Pool(args.pool_size, initializer=initializer) as pool:
        validation_results: Iterable[RuleValidationResult] = pool.imap_unordered(
            partial(sql_validate_single_rule, shared_cache, datasets, args, library_metadata),
            rules,
        )
        progress_handler: Callable = get_progress_displayer(args)
        results = progress_handler(rules, validation_results, results)

    # build all desired reports
    end = time.time()
    elapsed_time = end - start
    reporting_factory = ReportFactory(datasets, results, elapsed_time, args, data_service)
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


# TODO: fix this one first
# this is the tests entrypoint, CLI enters above where only the args are passed in
def sql_run_single_rule_validation(
    datasets,
    rule,
    define_xml: str = None,
    cache: InMemoryCacheService = None,
    standard: str = None,
    standard_version: str = "",
    standard_substandard: str = None,
    codelists=[],
) -> dict:
    # BS, this gets the DataService pushed from tests and ultimately the main command, containing all the standards
    datasets = [DummyDataset(dataset_data) for dataset_data in datasets]

    # get rid of cache and initialize standards in DB if not already present
    cache = cache or InMemoryCacheService()
    standard_details_cache_key = get_standard_details_cache_key(standard, standard_version, standard_substandard)
    variable_details_cache_key = get_library_variables_metadata_cache_key(
        standard, standard_version, standard_substandard
    )
    standard_metadata = cache.get(standard_details_cache_key)
    if standard_metadata:
        model_cache_key = get_model_details_cache_key_from_ig(standard_metadata)
        model_metadata = cache.get(model_cache_key)
    else:
        model_metadata = {}
    variable_codelist_cache_key = get_variable_codelist_map_cache_key(standard, standard_version, standard_substandard)
    ct_package_metadata = {}
    for codelist in codelists:
        ct_package_metadata[codelist] = cache.get(codelist)

    # Can we replace this?
    library_metadata = LibraryMetadataContainer(
        standard_metadata=standard_metadata,
        model_metadata=model_metadata,
        variables_metadata=cache.get(variable_details_cache_key),
        variable_codelist_map=cache.get(variable_codelist_cache_key),
        ct_package_metadata=ct_package_metadata,
        published_ct_packages=cache.get(PUBLISHED_CT_PACKAGES),
    )

    # this disappears - we initialize the SQL data service and with it have all available in test or main command
    data_service = DummyDataService.get_instance(
        cache,
        ConfigService(),
        standard=standard,
        standard_version=standard_version,
        standard_substandard=standard_substandard,
        data=datasets,
        define_xml=define_xml,
        library_metadata=library_metadata,
    )
    # refactor to get rid of cache - only needs access to the other stuff
    engine = SQLRulesEngine(
        cache,
        data_service,
        standard=standard,
        standard_version=standard_version,
        standard_substandard=standard_substandard,
        library_metadata=library_metadata,
    )

    # not sure why this happens here, think of where this should be happening
    rule = Rule.from_cdisc_metadata(rule)

    # finally we do something useful
    results = engine.sql_validate_single_rule(rule, datasets)
    return results
