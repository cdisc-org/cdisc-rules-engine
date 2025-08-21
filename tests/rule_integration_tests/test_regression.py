import os
import re
import json
from typing import Tuple
import pandas as pd
from unittest.mock import patch
from psycopg2 import errors

from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.test_dataset import TestDataset, TestVariableMetadata
from cdisc_rules_engine.utilities.ig_specification import IGSpecification
from scripts.run_sql_validation import sql_run_single_rule_validation
from scripts.run_validation import run_single_rule_validation


@patch("cdisc_rules_engine.services.data_services.DummyDataService.get_dataset_class")
def test_regression(mock_get_dataset_class, pytestconfig, get_core_rules_df, get_core_rule):
    mock_get_dataset_class.return_value = None
    regression_df = get_core_rules_df()
    local_path = "/Users/verisian/data/CORE/CDISC_Sharepoint_dump_20250806/unitTesting/"

    # set up SQL Engine
    ig_specs = {
        "standard": "SDTMIG",
        "standard_version": "3.4",
        "standard_substandard": None,
        "define_xml_version": None,
    }

    regression_json = []

    for _, row in regression_df.iterrows():
        cur_core_id = str(row["Core-ID"])
        cur_regression = initialize_regression_dict(row)
        if not cur_core_id or cur_core_id == "nan":
            cur_regression["core_id_is_null"] = True
            cur_regression["core_id_startswith_CORE"] = False
            cur_regression["in_cache"] = False
            cur_regression["rule_in_mltple_standards"] = []
        else:
            cur_regression["core_id_is_null"] = False
            if not cur_core_id.startswith("CORE-"):
                cur_regression["core_id_startswith_CORE"] = False
                cur_regression["in_cache"] = False
                cur_regression["rule_in_mltple_standards"] = []
            else:
                cur_regression["core_id_startswith_CORE"] = True
                rule = get_core_rule(cur_core_id)
                if not rule:
                    cur_regression["in_cache"] = False
                else:
                    cur_regression["in_cache"] = True
                    rule_ids = row["rids"]
                    for rid in rule_ids:
                        paths = get_data_paths_by_rule_id(local_path, row, rid)
                        if len(paths) == 1:
                            cur_regression["rule_in_mltple_standards"] = []
                            p = paths[0]
                            cur_regression["sharepoint_source"] = p.split("/")[-2]

                            for case in ["negative", "positive"]:
                                case_path = p + f"/{case}"
                                if os.path.exists(case_path):
                                    run_test_cases(cur_regression, case, case_path, ig_specs, rule)
                        elif len(paths) < 1:
                            cur_regression["rule_in_mltple_standards"] = []
                        else:
                            cur_regression["rule_in_mltple_standards"] = paths
        regression_json.append(cur_regression)

    # output rules.json
    with open(str(pytestconfig.rootpath) + "/tests/resources/rules/rules.json", "w", encoding="utf-8") as f:
        json.dump(regression_json, f, ensure_ascii=False, indent=4)


def initialize_regression_dict(row) -> dict:
    return {
        "core-id": row["Core-ID"] if pd.notna(row["Core-ID"]) and str(row["Core-ID"]).strip() else "unknown",
        "cdisc_rule_id": (
            row["CDISC Rule ID"] if pd.notna(row["CDISC Rule ID"]) and str(row["CDISC Rule ID"]).strip() else "unknown"
        ),
        "standard": (
            row["Standard Name"] if pd.notna(row["Standard Name"]) and str(row["Standard Name"]).strip() else "unknown"
        ),
        "executability": (
            row["Executability"] if pd.notna(row["Executability"]) and str(row["Executability"]).strip() else "unknown"
        ),
        "status": row["Status"] if pd.notna(row["Status"]) and str(row["Status"]).strip() else "unknown",
        "standard_source": row["standard_source"],
    }


def run_test_cases(
    cur_regression: dict,
    case: str,
    case_folder_path: str,
    ig_specs: IGSpecification,
    rule,
):
    two_digit_pattern = re.compile(r"^\d{2}$")
    cur_regression[f"{case}_folder_path"] = "/".join(case_folder_path.split("/")[-5:])
    test_case_folder_paths = [
        case_folder_path + "/" + name
        for name in os.listdir(case_folder_path)
        if os.path.isdir(os.path.join(case_folder_path, name)) and two_digit_pattern.match(name)
    ]

    test_case_regression = []
    for test_case_folder_path in test_case_folder_paths:

        try:
            test_case_file_path = find_data_file(test_case_folder_path + "/data")
            define_xml_file_path = find_define_xml_file_path(test_case_folder_path + "/data")
            # run engine
            engine_regression = {}
            run_regression_on_test_case(
                test_case_file_path,
                define_xml_file_path,
                engine_regression,
                ig_specs,
                rule,
            )
            test_case_regression.append(
                {
                    "/".join(test_case_folder_path.split("/")[-5:]): {
                        "test_case_xslx_file": "/".join(test_case_file_path.split("/")[-7:]),
                        "engine_regression": engine_regression,
                    }
                }
            )
        except FileNotFoundError:
            test_case_regression.append(
                {
                    "/".join(test_case_folder_path.split("/")[-5:]): {
                        "test_case_xslx_file": None,
                        "engine_regression": None,
                    }
                }
            )
    cur_regression[f"{case}_regressions"] = test_case_regression


def run_regression_on_test_case(
    data_file_path: str,
    define_xml_file_path: str,
    regression_errors: dict,
    ig_specs: IGSpecification,
    rule,
):
    can_process_dataset = False
    data_test_datasets = None

    # handle define-xmls
    if define_xml_file_path:
        regression_errors["define_xml_present"] = True
    else:
        regression_errors["define_xml_present"] = False

    # First phase: reading datasets from SharePoint XLSX
    try:
        data_test_datasets = sharepoint_xlsx_to_test_datasets(data_file_path)
        regression_errors["datasets_conversion"] = "SUCCESS"
        can_process_dataset = True
    except ValueError as e:
        err_msg = str(e)
        if err_msg == "Worksheet named 'Datasets' not found":
            regression_errors["datasets_conversion"] = "test_metadata_error - 'Datasets' sheet not found in xlsx file"
            regression_errors["datasets_import_sql"] = "FAIL"
            regression_errors["results_present_sql"] = False
            regression_errors["dataset_import_old"] = "FAIL"
            regression_errors["results_present_old"] = False
        elif err_msg.startswith("Error converting column"):
            regression_errors["datasets_conversion"] = f"column_convert_error - {err_msg}"
            regression_errors["datasets_import_sql"] = "FAIL"
            regression_errors["results_present_sql"] = False
            regression_errors["dataset_import_old"] = "FAIL"
            regression_errors["results_present_old"] = False
        elif err_msg.startswith("Unsupported column type:"):
            regression_errors["datasets_conversion"] = f"column_type_unsupported - {err_msg}"
            regression_errors["datasets_import_sql"] = "FAIL"
            regression_errors["results_present_sql"] = False
            regression_errors["dataset_import_old"] = "FAIL"
            regression_errors["results_present_old"] = False
        else:
            raise  # Not our expected error

    # Second phase: running validations if dataset can be processed
    if can_process_dataset:
        process_test_case_dataset(regression_errors, define_xml_file_path, data_test_datasets, ig_specs, rule)

    return None, None


def process_test_case_dataset(
    regression_errors: list, define_xml_file_path: str, data_test_datasets: list, ig_specs: IGSpecification, rule: dict
):
    try:
        # Execute rule in SQL engine
        ds = PostgresQLDataService.from_list_of_testdatasets(
            data_test_datasets, ig_specs, define_xml_path=define_xml_file_path
        )
        regression_errors["datasets_import_sql"] = "SUCCESS"
        sql_results = sql_run_single_rule_validation(data_service=ds, rule=rule)
        regression_errors["results_present_sql"] = True
        sql_regression = extract_sql_results_regression(sql_results)
        regression_errors["results_sql"] = sql_regression

        # Execute in old engine
        old_results = run_single_rule_validation(
            data_test_datasets,
            rule,
            define_xml=define_xml_file_path,
            standard=ig_specs["standard"],
            standard_version=ig_specs["standard_version"],
        )
        regression_errors["dataset_import_old"] = "SUCCESS"
        regression_errors["results_present_old"] = True
        old_regression = extract_sql_results_regression(old_results)
        regression_errors["results_old"] = old_regression

        regression_errors["old_vs_sql"] = old_vs_sql_regression_comparison(old_regression, sql_regression)

        return sql_results, old_results

    except ValueError as e:
        if str(e) == "Data list cannot be empty":
            regression_errors["datasets_import_sql"] = f"datasets_dataset_errors: {str(e)}"
        # if "column" in str(e) and "does not exist" in str(e):
        #     regression_errors["datasets_import_sql"] = f"pre_processor_error: {str(e)}"
        else:
            raise
    except errors.UndefinedColumn as e:
        if "column" in str(e) and "does not exist" in str(e):
            regression_errors["datasets_import_sql"] = f"pre_processor_error: {str(e)}"
        else:
            raise


def old_vs_sql_regression_comparison(old_results: list[dict], sql_results: list[dict]):
    comp_regression = {}
    # compare execution status
    for o_res in old_results:
        # find matching dataset/domain entries
        sql_res = next(
            (
                res
                for res in sql_results
                if res.get("dataset") == o_res.get("dataset") and res.get("domain") == o_res.get("domain")
            ),
            None,
        )
        if sql_res is not None:
            if o_res.get("execution_status") != sql_res.get("execution_status"):
                comp_regression["execution_status_match"] = False
            else:
                comp_regression["execution_status_match"] = True
                if not o_res.get("number_of_errors") != sql_res.get("number_of_errors"):
                    comp_regression["number_of_errors_match"] = False
                else:
                    comp_regression["number_of_errors_match"] = True
                    comp_regression["deep_diff"] = compare_error_lists(o_res.get("errors"), sql_res.get("errors"))
        else:
            comp_regression["execution_status_match"] = False
            comp_regression["number_of_errors_match"] = False

    return comp_regression


def compare_error_lists(old_errors, sql_errors):
    set1 = {json.dumps(item, sort_keys=True) for sublist in old_errors for item in sublist}
    set2 = {json.dumps(item, sort_keys=True) for sublist in sql_errors for item in sublist}
    diff_serialized = set1.symmetric_difference(set2)
    return [json.loads(item) for item in diff_serialized]


def extract_sql_results_regression(results):
    res_regression = []
    for _, res in results.items():
        domain_res_regression = {
            "dataset": res[0].get("dataset", ""),
            "domain": res[0].get("domain", ""),
            "execution_status": res[0].get("executionStatus", ""),
            "execution_message": res[0].get("message", ""),
            "number_errors": len(res[0].get("errors")),
        }
        if res[0].get("executionStatus", "") == "execution_error":
            domain_res_regression["errors"] = (
                [{"error": error.get("error"), "message": error.get("message")} for error in res[0].get("errors")],
            )
        elif res[0].get("executionStatus", "") == "skipped":
            domain_res_regression["errors"] = []
        elif res[0].get("executionStatus", "") == "success":
            domain_res_regression["errors"] = (
                [
                    {
                        "row": error.get("row"),
                        "SEQ": error.get("SEQ"),
                        "USUBJID": error.get("USUBJID"),
                        "value": error.get("value"),
                    }
                    for error in res[0].get("errors")
                ],
            )
        else:
            domain_res_regression["errors"] = [{"error": "unknown execution status"}]
        res_regression.append(domain_res_regression)
    return res_regression


def get_data_paths_by_rule_id(local_path: str, row: pd.Series, rid: str) -> list[str]:
    paths = []
    if "SDTMIG" in row["std"]:
        paths.extend(
            find_dirs(
                local_path + "SDTMIG",
                rid,
                case_insensitive=True,
            )
        )
    wanted = {"ADAMIG", "ADaMIG", "ADaMIG-MD", "ADTTE"}
    if any(s in wanted for s in row["std"]):
        paths.extend(
            find_dirs(
                local_path + "ADAMIG",
                rid,
                case_insensitive=True,
            )
        )
    paths.extend(
        find_dirs(
            local_path + "FDA Business Rules",
            rid,
            case_insensitive=True,
        )
    )
    paths.extend(
        find_dirs(
            local_path + "FDA Validator Rules",
            rid,
            case_insensitive=True,
        )
    )
    return paths


def sharepoint_xlsx_to_test_datasets(path: str) -> list[TestDataset]:
    # Step 1: Read the "Datasets" sheet
    xlsx_data = pd.ExcelFile(path)
    datasets_df = pd.read_excel(xlsx_data, sheet_name="Datasets")

    # Step 2: Initialize list to store TestDataset objects
    test_datasets = []

    # Step 3: Iterate over each row in the "Datasets" sheet
    for _, row in datasets_df.iterrows():
        filename = row["Filename"]
        label = row["Label"]

        # Step 4: Read the sheet for the dataset
        if filename in xlsx_data.sheet_names:
            dataset_df = pd.read_excel(xlsx_data, sheet_name=filename)

            # Step 5: Extract variable details (name, label, type, length)
            variables, col_type_dict = extract_variables(dataset_df)

            # Step 6: Extract data (rest of the rows)
            data = extract_data(filename, col_type_dict, dataset_df)

            # Step 7: Create a TestDataset object and append it to the list
            test_datasets.append(
                TestDataset(
                    filename=filename,
                    filepath=filename,
                    name=filename.split(".")[0],
                    label=label,
                    variables=variables,
                    records=data,
                )
            )

    return test_datasets


def extract_variables(dataset_df: pd.DataFrame) -> Tuple[list[TestVariableMetadata, dict]]:
    variables = []
    col_type_dict = {}
    for col in dataset_df.columns:
        var_name = col  # Name from row 0
        if col.startswith("Unnamed:"):
            continue
        var_label = str(dataset_df[col].iloc[0])  # Label from row 1
        var_type = str(dataset_df[col].iloc[1])  # Type from row 2
        var_length = dataset_df[col].iloc[2]  # Length from row 3
        var_format = ""  # Format is always empty

        # Create a variable dictionary
        variables.append(
            {"name": var_name, "label": var_label, "type": var_type, "length": var_length, "format": var_format}
        )

        # collect appropriate column type for SQL
        col_type_dict[var_name] = var_type

    return variables, col_type_dict


def extract_data(filename: str, col_type_dict: dict, dataset_df: pd.DataFrame) -> dict:
    data = {}
    for col in dataset_df.columns:
        if col.startswith("Unnamed:"):
            continue
        column_name = col  # Column name from row 0
        column_values = dataset_df[col].iloc[3:].tolist()  # All values below row 3

        # Preprocess the column values based on the column type
        if col_type_dict[column_name].lower() == "num":
            try:
                column_values = [None if pd.isna(val) else float(val) for val in column_values]
            except ValueError as e:
                raise ValueError(f"Error converting column '{column_name}' in table '{filename}' to numeric: {e}")
        elif col_type_dict[column_name].lower() == "char":
            try:
                column_values = ["" if pd.isna(val) else str(val) for val in column_values]
            except ValueError as e:
                raise ValueError(f"Error converting column '{column_name}' in table '{filename}' to string: {e}")
        else:
            raise ValueError(f"Unsupported column type: {col_type_dict[column_name]} for rule")

        # Store the column name and its values in the data dictionary
        data[column_name] = column_values

    return data


def find_dirs(root, target_name, case_insensitive=False) -> list[str]:
    matches = []
    for d in os.listdir(root):
        if (d == target_name) or (case_insensitive and d.lower() == target_name.lower()):
            matches.append(os.path.join(root, d))
    return matches


def find_max_dir(root) -> str:
    max = 0
    max_d = ""
    try:
        for d in os.listdir(root):
            if d.isdigit():
                d_int = int(d)
                if d_int >= max:
                    max = d_int
                    max_d = os.path.join(root, d)
        return max_d
    except FileNotFoundError:
        return ""


def find_data_file(path: str) -> str:
    if not path:
        return ""
    accepted_extensions = ["xls", "xlsx"]
    try:
        for filename in os.listdir(path):
            full_path = os.path.join(path, filename)
            extension = filename.split(".")[-1].lower()
            if os.path.isfile(full_path) and extension in accepted_extensions:
                return path + "/" + filename
    except FileNotFoundError:
        return ""
    return ""


def find_define_xml_file_path(path: str) -> str:
    try:
        for filename in os.listdir(path):
            full_path = os.path.join(path, filename)
            if os.path.isfile(full_path) and filename.lower() == "define.xml":
                return full_path
    except FileNotFoundError:
        return ""
    return ""


@patch("cdisc_rules_engine.services.data_services.DummyDataService.get_dataset_class")
def test_rule_existing_rule(mock_get_dataset_class, get_sample_lb_rule, get_sample_lb_dataset):
    mock_get_dataset_class.return_value = None
    ig_specs = {
        "standard": "SDTMIG",
        "standard_version": "3.4",
        "standard_substandard": None,
        "define_xml_version": None,
    }
    ds = PostgresQLDataService.from_list_of_testdatasets([get_sample_lb_dataset], ig_specs)
    data = sql_run_single_rule_validation(data_service=ds, rule=get_sample_lb_rule)

    assert "LB" in data
    assert len(data["LB"]) == 1
    assert data["LB"][0]["message"] == "LBSEQ greater than 0"
    assert len(data["LB"][0]["errors"]) == 2
