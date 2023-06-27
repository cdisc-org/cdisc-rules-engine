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
    fill_cache_with_dictionaries,
    fill_cache_with_provided_data,
    get_cache_service,
    get_define_metadata,
)
from cdisc_rules_engine.utilities.utils import get_directory_path
from cdisc_rules_engine.enums.progress_parameter_options import ProgressParameterOptions

"""
Sync manager used to manage instances of the cache between processes.
Cache types are registered to this manager, and only one instance of the
cache is created at startup and provided to each process.
"""


class CacheManager(SyncManager):
    pass


def validate_single_rule(
    cache, path, args, datasets, define_metadata, rule: dict = None
):
    set_log_level("ERROR")
    rule["conditions"] = ConditionCompositeFactory.get_condition_composite(
        rule["conditions"]
    )
    # call rule engine
    engine = RulesEngine(
        cache=cache,
        standard=args.standard,
        standard_version=args.version,
        ct_package=args.controlled_terminology_package,
        meddra_path=args.meddra,
        whodrug_path=args.whodrug,
        define_metadata=args.define_metadata,
    )
    validated_domains = set()
    results = []
    directory = get_directory_path(args.dataset_path)
    if rule.get("sensitivity").lower() == "study":
        results.append(
            engine.test_validation(
                rule,
                directory,
                datasets,
                None,
                define_metadata,
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
                    define_metadata,
                )
                results.append(validated_result)
                engine_logger.info(f"Done validating {dataset.domain}")
    results = list(itertools.chain(*results))
    return RuleValidationResult(rule, results)


def set_log_level(level: str):
    if level == "disabled":
        engine_logger.disabled = True
    else:
        engine_logger.setLevel(level)


def test(args: TestArgs):
    set_log_level("ERROR")
    # fill cache
    CacheManager.register("RedisCacheService", RedisCacheService)
    CacheManager.register("InMemoryCacheService", InMemoryCacheService)
    manager = CacheManager()
    manager.start()
    shared_cache = get_cache_service(manager)
    shared_cache = fill_cache_with_provided_data(shared_cache, args)

    # install dictionaries if needed
    fill_cache_with_dictionaries(shared_cache, args)
    with open(args.rule, "r") as f:
        rules = [Rule.from_cdisc_metadata(json.load(f))]
    data_service = DataServiceFactory(config, shared_cache).get_data_service()
    with open(args.dataset_path, "r") as f:
        data_json = json.load(f)
    datasets = [DummyDataset(data) for data in data_json.get("datasets", [])]
    define_metadata = get_define_metadata(define_file_path=args.dataset_path)

    print(f"\nxxx1000: DefineXML: {define_metadata}")

    start = time.time()
    results = []
    # run each rule in a separate process
    with Pool(10) as pool:
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
                    define_metadata,
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
        [args.dataset_path],
        None,
        os.path.join("resources", "templates", "report-template.xlsx"),
        args.standard,
        args.version,
        args.controlled_terminology_package,
        output_file,
        ["XLSX"],
        None,
        args.define_version,
        "xpt",
        args.meddra,
        args.whodrug,
        rules,
        ProgressParameterOptions.BAR.value,
    )
    reporting_factory = ReportFactory(
        [args.dataset_path], results, elapsed_time, validation_args, data_service
    )
    reporting_services: List[BaseReport] = reporting_factory.get_report_services()
    for reporting_service in reporting_services:
        reporting_service.write_report()
    print(f"Output: {output_file}")
