import json
import os

from dotenv import load_dotenv

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext
from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths
from rule_regression.operator_analysis import generate_operators_analysis_report
from rule_regression.regression import (
    delete_files_in_directory,
    run_single_rule_regression,
)
from scripts.run_sql_validation import sql_run_single_rule_validation

load_dotenv()

TEST_CACHE_PATH = os.path.join(os.path.dirname(__file__), "..", "..", DefaultFilePaths.CACHE.value)


def test_regression_all_rules(pytestconfig, get_core_rules_df, get_core_rule):
    regression_df = get_core_rules_df()
    regression_json = []
    all_check_operators = set()

    data_service = PostgresQLDataService.instance(cache_path=TEST_CACHE_PATH)
    try:
        for _, row in regression_df.iterrows():
            rule_reg = run_single_rule_regression(row, get_core_rule, data_service=data_service)
            regression_json.append(rule_reg)

            # Extract and add check operators to the set
            check_operators = rule_reg.get("check_operators", [])
            if check_operators:
                all_check_operators.update(check_operators)
    finally:
        data_service.pgi.close()

    # Generate and save the operators analysis report
    generate_operators_analysis_report(all_check_operators)

    with open(
        str(pytestconfig.rootpath) + "/tests/resources/rules/rules.json",
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(regression_json, f, ensure_ascii=False, indent=4)


def test_regression_single_rule_DEV(pytestconfig, get_core_rules_df, get_core_rule):
    rule_id = os.getenv("CURRENT_RULE_DEV", "")
    assert rule_id
    regression_df = get_core_rules_df()
    data_service = PostgresQLDataService.instance(cache_path=TEST_CACHE_PATH)
    rule_reg = run_single_rule_regression(
        regression_df[regression_df["Core-ID"] == rule_id].iloc[0], get_core_rule, data_service=data_service
    )
    output_folder = str(pytestconfig.rootpath) + "/tests/resources/rules/dev/"
    delete_files_in_directory(output_folder)
    with open(f"{output_folder}dev.json", "w", encoding="utf-8") as f:
        json.dump(rule_reg, f, ensure_ascii=False, indent=4)
    # output_engine_results_json(pytestconfig, get_core_rules_df, get_core_rule, "old")
    # output_engine_results_json(pytestconfig, get_core_rules_df, get_core_rule, "sql")


def test_regression_single_case_DEV(pytestconfig, get_core_rules_df, get_core_rule):
    rule_id = os.getenv("CURRENT_RULE_DEV", "")
    case_path = os.getenv("CURRENT_RULE_DEV_CASE", "")
    assert rule_id
    regression_df = get_core_rules_df()
    data_service = PostgresQLDataService.instance(cache_path=TEST_CACHE_PATH)
    rule_reg = run_single_rule_regression(
        regression_df[regression_df["Core-ID"] == rule_id].iloc[0],
        get_core_rule,
        target_case=case_path,
        data_service=data_service,
    )
    output_folder = str(pytestconfig.rootpath) + "/tests/resources/rules/dev/"
    delete_files_in_directory(output_folder)
    with open(f"{output_folder}dev.json", "w", encoding="utf-8") as f:
        json.dump(rule_reg, f, ensure_ascii=False, indent=4)
    # output_engine_results_json(pytestconfig, get_core_rules_df, get_core_rule, "old")
    # output_engine_results_json(pytestconfig, get_core_rules_df, get_core_rule, "sql")


def test_rule_existing_rule(get_sample_lb_rule, get_sample_lb_dataset):
    class DummyStandardsContext(DefaultStandardsContext):
        def __init__(self):
            self.standard = "SDTMIG"
            self.standard_version = "3.4"
            self.standard_substandard = None
            self.define_xml_version = None

        def transform_dataset_metadata(self, source):
            return source

    ds = PostgresQLDataService.from_list_of_testdatasets(
        [get_sample_lb_dataset], DummyStandardsContext(), cache_path=TEST_CACHE_PATH
    )
    ds.datasets = [
        BaseDatasetMetadata(**{k: v for k, v in ds.datasets[0].__dict__.items()}, domain=ds.datasets[0].name.upper())
    ]
    data = sql_run_single_rule_validation(
        data_service=ds, rule=get_sample_lb_rule, standards_context=DummyStandardsContext()
    )

    assert "lb" in data
    assert len(data["lb"]) == 1
    assert data["lb"][0]["message"] == "LBSEQ less than maximum value"
    assert len(data["lb"][0]["errors"]) == 1


def test_regression_all_rules_pgserver(pytestconfig, get_core_rules_df, get_core_rule):
    regression_df = get_core_rules_df()
    regression_json = []
    all_check_operators = set()

    data_service = PostgresQLDataService.instance(use_pgserver=True, cache_path=TEST_CACHE_PATH)
    try:
        for _, row in regression_df.iterrows():
            rule_reg = run_single_rule_regression(row, get_core_rule, use_pgserver=True, data_service=data_service)
            regression_json.append(rule_reg)

            # Extract and add check operators to the set
            check_operators = rule_reg.get("check_operators", [])
            if check_operators:
                all_check_operators.update(check_operators)
    finally:
        data_service.pgi.close()

    # Generate and save the operators analysis report
    generate_operators_analysis_report(all_check_operators)

    with open(
        str(pytestconfig.rootpath) + "/tests/resources/rules/rules.json",
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(regression_json, f, ensure_ascii=False, indent=4)


def test_regression_single_rule_pgserver_DEV(pytestconfig, get_core_rules_df, get_core_rule):
    rule_id = os.getenv("CURRENT_RULE_DEV", "")
    assert rule_id
    regression_df = get_core_rules_df()
    data_service = PostgresQLDataService.instance(cache_path=TEST_CACHE_PATH)
    rule_reg = run_single_rule_regression(
        regression_df[regression_df["Core-ID"] == rule_id].iloc[0],
        get_core_rule,
        use_pgserver=True,
        data_service=data_service,
    )
    output_folder = str(pytestconfig.rootpath) + "/tests/resources/rules/dev/"
    delete_files_in_directory(output_folder)
    with open(f"{output_folder}dev.json", "w", encoding="utf-8") as f:
        json.dump(rule_reg, f, ensure_ascii=False, indent=4)
    # output_engine_results_json(pytestconfig, get_core_rules_df, get_core_rule, "old")
    # output_engine_results_json(pytestconfig, get_core_rules_df, get_core_rule, "sql")
