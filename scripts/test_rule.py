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
    fill_cache_with_dictionaries,
    get_cache_service,
    get_datasets,
    get_library_metadata_from_cache,
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
    cache,
    path,
    args,
    datasets,
    library_metadata: LibraryMetadataContainer,
    rule: dict = None,
):
    set_log_level("ERROR")
    rule["conditions"] = ConditionCompositeFactory.get_condition_composite(
        rule["conditions"]
    )
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
        validate_xml=args.validate_xml,
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
    library_metadata: LibraryMetadataContainer = get_library_metadata_from_cache(args)
    # install dictionaries if needed
    fill_cache_with_dictionaries(shared_cache, args)
    with open(args.rule, "r") as f:
        rules = [Rule.from_cdisc_metadata(json.load(f))]
    with open(args.dataset_path, "r") as f:
        data_json = json.load(f)
    datasets = [DummyDataset(data) for data in data_json.get("datasets", [])]
    data_service_factory = DataServiceFactory(
        config, shared_cache, args.standard, args.version
    )
    dummy_data_service = data_service_factory.get_dummy_data_service(datasets)
    data_service = data_service_factory.get_data_service()

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
        args.define_xml_path,
    )
    reporting_factory = ReportFactory(
        get_datasets(dummy_data_service, [dataset.filename for dataset in datasets]),
        results,
        elapsed_time,
        validation_args,
        data_service,
    )
    reporting_services: List[BaseReport] = reporting_factory.get_report_services()
    for reporting_service in reporting_services:
        reporting_service.write_report(define_xml_path=args.define_xml_path)
    print(f"Output: {output_file}")
