import itertools
import time
import click
from datetime import datetime
import json
import os
from functools import partial
from multiprocessing import Pool
from multiprocessing.managers import SyncManager
from typing import List
from cdisc_rules_engine.config import config
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
from cdisc_rules_engine.services.data_services import DataServiceFactory
from cdisc_rules_engine.dummy_models.dummy_dataset import DummyDataset
from cdisc_rules_engine.services.reporting import BaseReport, ReportFactory
from cdisc_rules_engine.models.test_args import TestArgs
from cdisc_rules_engine.models.rule import Rule
from cdisc_rules_engine.utilities.utils import generate_report_filename
from scripts.script_utils import (
    get_datasets,
    fill_cache_with_dictionaries,
    get_cache_service,
    get_library_metadata_from_cache,
)
from cdisc_rules_engine.enums.progress_parameter_options import ProgressParameterOptions

"""
Sync manager used to manage instances of the cache between processes.
Cache types are registered to this manager, and only one instance of the
cache is created at startup and provided to each process.
"""


class CacheManager(SyncManager):
    pass


def validate_single_rule(
    cache,
    path,
    args,
    datasets,
    library_metadata: LibraryMetadataContainer,
    rule: dict = None,
):
    set_log_level(args)
    rule["conditions"] = ConditionCompositeFactory.get_condition_composite(
        rule["conditions"]
    )
    # call rule engine
    engine = RulesEngine(
        cache=cache,
        standard=args.standard,
        standard_version=args.version.replace(".", "-"),
        standard_substandard=args.substandard,
        ct_packages=args.controlled_terminology_package,
        external_dictionaries=args.external_dictionaries,
        define_xml_path=args.define_xml_path,
        library_metadata=library_metadata,
        validate_xml=args.validate_xml,
        dataset_paths=args.dataset_paths,
    )
    validated_domains = set()
    results = []
    directory = os.path.dirname(args.dataset_paths[0])

    if rule.get("sensitivity").lower() == "study":
        results.append(
            engine.test_validation(
                rule,
                directory,
                datasets,
                "study",
            )
        )
        engine_logger.info("Done validating the rule for the study.")
    else:
        for dataset in datasets:
            # Check if the domain has been validated before
            # This addresses the case where a domain is split
            # and appears multiple times within the list of datasets
            if dataset.domain not in validated_domains:
                validated_domains.add(dataset.domain)
                validated_result = engine.test_validation(
                    rule,
                    os.path.join(directory, dataset.filename),
                    datasets,
                    dataset.domain,
                )
                results.append(validated_result)
                engine_logger.info(f"Done validating {dataset.domain}")
    results = list(itertools.chain(*results))
    return RuleValidationResult(rule, results)


def initialize_logger(disabled, log_level):
    if disabled:
        engine_logger.disabled = True
    else:
        engine_logger.disabled = False
        engine_logger.setLevel(log_level)


def set_log_level(args):
    if args.log_level.lower() == "disabled":
        engine_logger.disabled = True
    else:
        engine_logger.setLevel(args.log_level.lower())


def test(args: TestArgs):
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
    dictionary_versions = fill_cache_with_dictionaries(shared_cache, args)
    with open(args.rule, "r", encoding="utf-8") as f:
        rules = [Rule.from_cdisc_metadata(json.load(f))]
    data_service_factory = DataServiceFactory(
        config, shared_cache, args.standard, args.version, args.substandard
    )
    data_service = data_service_factory.get_data_service()
    datasets = []
    for dataset_path in args.dataset_paths:
        try:
            with open(dataset_path, "r") as f:
                data_json = json.load(f)
                datasets.extend(
                    [DummyDataset(data) for data in data_json.get("datasets", [])]
                )
        except Exception as e:
            engine_logger.info(f"Dataset {dataset_path} is not encoded in {e}")
    if not datasets:
        engine_logger.info(
            "No datasets loaded from JSON files, attempting to load using data service"
        )
        try:
            datasets = [
                DummyDataset(dataset)
                for dataset in get_datasets(data_service, args.dataset_paths)
            ]
            for dataset_path in args.dataset_paths:
                filename = os.path.basename(dataset_path).lower()
                matching_dataset = next(
                    dataset
                    for dataset in datasets
                    if dataset.filename.lower() == filename
                )
                df = data_service.get_dataset(dataset_name=dataset_path)
                matching_dataset.data = df.data
        except Exception as e:
            engine_logger.error(f"Data service failed to load datasets: {e}")
    dummy_data_service = data_service_factory.get_dummy_data_service(datasets)
    start = time.time()
    results = []
    # instantiate logger in each child process to maintain log level
    initializer = partial(
        initialize_logger, engine_logger.disabled, engine_logger._logger.level
    )
    # run each rule in a separate process
    with Pool(10, initializer=initializer) as pool:
        with click.progressbar(
            length=len(rules),
            fill_char=click.style("\u2588", fg="green"),
            empty_char=click.style("-", fg="white", dim=True),
            show_eta=False,
        ) as bar:
            for rule_result in pool.imap_unordered(
                partial(
                    validate_single_rule,
                    shared_cache,
                    "",
                    args,
                    datasets,
                    library_metadata,
                ),
                rules,
            ):
                results.append(rule_result)
                bar.update(1)

    end = time.time()
    elapsed_time = end - start
    output_file = generate_report_filename(datetime.now().isoformat())

    validation_args = Validation_args(
        None,
        None,
        args.dataset_paths,
        None,
        os.path.join("resources", "templates", "report-template.xlsx"),
        args.standard,
        args.version,
        args.substandard,
        args.controlled_terminology_package,
        output_file,
        ["XLSX"],
        None,
        args.define_version,
        args.external_dictionaries,
        rules,
        None,
        None,
        None,
        ProgressParameterOptions.BAR.value,
        args.define_xml_path,
    )
    reporting_factory = ReportFactory(
        dummy_data_service.get_datasets(),
        results,
        elapsed_time,
        validation_args,
        data_service,
    )
    reporting_services: List[BaseReport] = reporting_factory.get_report_services()
    for reporting_service in reporting_services:
        reporting_service.write_report(
            define_xml_path=args.define_xml_path,
            dictionary_versions=dictionary_versions,
        )
    print(f"Output: {output_file}")
