import itertools
import time
from typing import Callable, List
from warnings import simplefilter

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.enums.progress_parameter_options import ProgressParameterOptions
from cdisc_rules_engine.models.library_metadata_container import (
    LibraryMetadataContainer,
)
from cdisc_rules_engine.models.rule_conditions import ConditionCompositeFactory
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.models.sql_rule import SQLRule
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.services import logger as engine_logger
from cdisc_rules_engine.services.reporting import BaseReport, ReportFactory
from cdisc_rules_engine.sql_rules_engine import SQLRulesEngine

# from cdisc_rules_engine.utilities.utils import (
#     get_library_variables_metadata_cache_key,
#     get_model_details_cache_key_from_ig,
#     get_standard_details_cache_key,
#     get_variable_codelist_map_cache_key,
# )
from cdisc_rules_engine.standards.base_standards_context import BaseStandardsContext
from cdisc_rules_engine.standards.standards_factory import StandardsFactory
from cdisc_rules_engine.utilities.progress_displayers import get_progress_displayer
from cdisc_rules_engine.utilities.sql_rule_processor import SQLRuleProcessor
from scripts.script_utils import (
    get_library_metadata_from_cache,
    get_rules,
)

# from cdisc_rules_engine.constants.cache_constants import PUBLISHED_CT_PACKAGES

simplefilter(action="ignore", category=FutureWarning)  # Suppress warnings coming from numpy


def sql_validate_single_rule(
    engine: SQLRulesEngine,
    args: Validation_args,
    rule: dict = None,
):
    if not SQLRuleProcessor.valid_rule_structure(rule):
        engine_logger.error(f"Invalid rule structure: {rule}")
        return RuleValidationResult(rule, [])

    rule["conditions"] = ConditionCompositeFactory.get_condition_composite(rule["conditions"])
    # call rule engine
    results = engine.sql_validate_single_rule(rule)
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


def run_sql_validation(args: Validation_args):
    set_log_level(args)

    rules = get_rules(args)
    library_metadata: LibraryMetadataContainer = get_library_metadata_from_cache(args)
    standard = args.standard
    standard_version = args.version.replace(".", "-")
    standard_substandard = args.substandard

    standards_context = StandardsFactory.get_standards_context(
        standard, standard_version, standard_substandard, library_metadata
    )

    data_service = PostgresQLDataService.from_dataset_paths(args.dataset_paths)

    engine = SQLRulesEngine(data_service=data_service, standards_context=standards_context)

    engine_logger.info(f"Running {len(rules)} rules against {len(data_service.datasets)} datasets")
    start = time.time()
    results = []

    # instantiate logger in each child process to maintain log level
    # initializer = partial(initialize_logger, engine_logger.disabled, engine_logger._logger.level)
    # run each rule in a separate process
    # with Pool(args.pool_size, initializer=initializer) as pool:
    #     validation_results: Iterable[RuleValidationResult] = pool.imap_unordered(
    #         partial(sql_validate_single_rule, engine, args),
    #         rules,
    #     )
    #     progress_handler: Callable = get_progress_displayer(args)
    #     results = progress_handler(rules, validation_results, results)
    def run():
        for rule in rules:
            rule_result = sql_validate_single_rule(engine, args, rule)
            yield rule_result

    progress_handler: Callable = get_progress_displayer(args)
    iterable = run()
    progress_handler(rules, iterable, results)

    # build all desired reports
    end = time.time()
    elapsed_time = end - start
    reporting_factory = ReportFactory(data_service.datasets, results, elapsed_time, args, data_service)
    reporting_services: List[BaseReport] = reporting_factory.get_report_services()
    for reporting_service in reporting_services:
        reporting_service.write_report(
            define_xml_path=args.define_xml_path,
            # dictionary_versions=dictionary_versions,
        )
    print(f"Output: {args.output}")


# TODO: fix this one first
# this is the tests entrypoint, CLI enters above where only the args are passed in
def sql_run_single_rule_validation(
    data_service: PostgresQLDataService, rule: dict, standards_context: BaseStandardsContext
) -> dict:
    return SQLRulesEngine(data_service=data_service, standards_context=standards_context).sql_validate_single_rule(
        SQLRule.from_cdisc_metadata(rule)
    )
