import itertools
import sys
import time
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from typing import List
from warnings import simplefilter

import click

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
from cdisc_rules_engine.utilities.ingestion_progress import get_ingestion_progress_reporter
from cdisc_rules_engine.utilities.report_progress import get_report_progress_reporter
from cdisc_rules_engine.utilities.rule_progress import get_rule_progress_reporter
from cdisc_rules_engine.utilities.rule_required_inputs import (
    get_missing_inputs,
    get_missing_inputs_summary,
    get_not_applicable_reason,
    get_supplied_inputs,
    is_not_applicable,
)
from cdisc_rules_engine.utilities.sql_rule_processor import SQLRuleProcessor
from scripts.script_utils import (
    get_library_metadata_from_cache,
    get_rules,
)

# from cdisc_rules_engine.constants.cache_constants import PUBLISHED_CT_PACKAGES

simplefilter(action="ignore", category=FutureWarning)  # Suppress warnings coming from numpy


NO_RULES_MESSAGE = "No rules were found with the specified parameters."
NO_RULES_PROMPT = (
    f"{NO_RULES_MESSAGE} If this was deliberate, and you want to do a test run with no rules, "
    "please type 'Y'. Any other key will exit"
)


def confirm_run_without_rules() -> bool:
    """
    Asks the user whether to continue a run that has no rules. Returns True only if they type Y.
    Without a terminal to answer the prompt (e.g. CI or piped input), the run does not continue.
    """
    if not sys.stdin.isatty():
        click.echo(f"{NO_RULES_MESSAGE} Exiting.", err=True)
        return False
    click.echo(f"{NO_RULES_PROMPT}: ", nl=False)
    key = click.getchar()
    click.echo(key)
    return key.upper() == "Y"


def sql_validate_single_rule(
    engine: SQLRulesEngine,
    args: Validation_args,
    rule: dict = None,
    missing_inputs: List[str] = None,
):
    if not SQLRuleProcessor.valid_rule_structure(rule):
        engine_logger.error(f"Invalid rule structure: {rule}")
        return RuleValidationResult(rule, [])

    rule["conditions"] = ConditionCompositeFactory.get_condition_composite(rule["conditions"])
    # call rule engine; a rule that needs an input the user didn't supply is reported as not applicable
    not_applicable_reason = get_not_applicable_reason(missing_inputs) if missing_inputs else None
    results = engine.sql_validate_single_rule(rule, not_applicable_reason=not_applicable_reason)
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


def get_dictionary_versions(dictionary_metadata: dict) -> dict:
    """Maps dictionary type -> version string for the report's Conformance Details."""
    versions = {}
    for dictionary_type, metadata in (dictionary_metadata or {}).items():
        if dictionary_type == DictionaryTypes.SNOMED.value:
            versions[dictionary_type] = f"MAIN/{metadata.edition}/{metadata.version}"
        elif dictionary_type == DictionaryTypes.WHODRUG.value:
            versions[dictionary_type] = metadata.version_date
        else:
            versions[dictionary_type] = metadata.version
    return versions


def run_sql_validation(args: Validation_args, in_memory_postgres: bool = False):
    set_log_level(args)

    rules = get_rules(args)
    if not rules and not confirm_run_without_rules():
        sys.exit(1)
    library_metadata: LibraryMetadataContainer = get_library_metadata_from_cache(args, sql=True)
    standard = args.standard
    standard_version = args.version.replace(".", "-")
    standard_substandard = args.substandard

    standards_context = StandardsFactory.get_standards_context(
        standard, standard_version, standard_substandard, library_metadata
    )

    data_service = PostgresQLDataService.from_dataset_paths(
        args.dataset_paths,
        standards_context=standards_context,
        codelists=library_metadata.get_all_ct_package_metadata(),
        provided_codelists=args.controlled_terminology_package,
        extensible_terms=library_metadata.get_ct_package_metadata("extensible"),
        external_dictionaries=args.external_dictionaries,
        cache_path=args.cache,
        define_xml_path=args.define_xml_path,
        stf_file_path=args.stf_file_path,
        sql_namespace=args.sql_namespace,
        use_pgserver=in_memory_postgres,
        progress_reporter=get_ingestion_progress_reporter(args.progress),
    )

    rule_progress_reporter = get_rule_progress_reporter(args.progress)
    engine = SQLRulesEngine(
        data_service=data_service,
        standards_context=standards_context,
        progress_reporter=rule_progress_reporter,
        max_errors_per_dataset=args.max_errors_per_dataset,
    )

    engine_logger.info(f"Running {len(rules)} rules against {len(data_service.datasets)} datasets")
    start = time.time()
    results = []
    supplied_inputs = get_supplied_inputs(
        data_service.define_xml_path, data_service.stf_file_path, data_service.dictionary_metadata.keys()
    )
    missing_inputs_by_rule = {}

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
    rule_progress_reporter.start(len(rules))
    try:
        for rule_index, rule in enumerate(rules):
            rule_progress_reporter.start_rule(rule.get("core_id"), rule_index, len(rules))
            missing_inputs = get_missing_inputs(rule, supplied_inputs)
            result = sql_validate_single_rule(engine, args, rule, missing_inputs)
            # count the rule only if a dataset in its scope was reported as not applicable
            if missing_inputs and any(is_not_applicable(entry) for entry in result.results):
                missing_inputs_by_rule[rule.get("core_id")] = missing_inputs
            results.append(result)
            rule_progress_reporter.end_rule()
    finally:
        rule_progress_reporter.finish()

    data_service.pgi.wipe()

    # build all desired reports
    end = time.time()
    elapsed_time = end - start
    reporting_factory = ReportFactory(data_service.datasets, results, elapsed_time, args, data_service)
    reporting_services: List[BaseReport] = reporting_factory.get_report_services()
    report_start = time.time()
    for reporting_service in reporting_services:
        reporting_service.write_report(
            define_xml_path=args.define_xml_path,
            dictionary_versions=get_dictionary_versions(data_service.dictionary_metadata),
            progress_reporter=get_report_progress_reporter(args.progress),
        )
    report_elapsed = time.time() - report_start
    print(f"Validation took {elapsed_time:.1f}s, report generation took {report_elapsed:.1f}s")
    missing_inputs_summary = get_missing_inputs_summary(missing_inputs_by_rule)
    if missing_inputs_summary:
        print(missing_inputs_summary)
    print(f"Output: {args.output}")


# TODO: fix this one first
# this is the tests entrypoint, CLI enters above where only the args are passed in
def sql_run_single_rule_validation(
    data_service: PostgresQLDataService, rule: dict, standards_context: BaseStandardsContext
) -> dict:
    return SQLRulesEngine(data_service=data_service, standards_context=standards_context).sql_validate_single_rule(
        SQLRule.from_cdisc_metadata(rule)
    )
