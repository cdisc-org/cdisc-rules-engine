import json
import os
import re
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from deepdiff import DeepDiff
from psycopg2 import errors

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths
from cdisc_rules_engine.models.test_dataset import TestDataset, TestVariableMetadata
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.utilities.ig_specification import IGSpecification
from cdisc_rules_engine.utilities.sql_rule_processor import SQLRuleProcessor
from scripts.run_sql_validation import sql_run_single_rule_validation
from scripts.run_validation import run_single_rule_validation
from scripts.script_utils import get_library_metadata_from_cache

RULE_DEPTH = 2
TYPE_DEPTH = RULE_DEPTH + 1
CASE_DEPTH = TYPE_DEPTH + 1
DATA_DEPTH = CASE_DEPTH + 2

METADATA_CACHE = {}


def run_single_rule_regression(row: pd.Series, get_core_rule, target_case: Optional[str] = None) -> list:
    ig_specs = {
        "standard": "sdtmig",
        "standard_version": "3.4",
        "standard_substandard": None,
        "define_xml_version": None,
    }
    cur_core_id = str(row["Core-ID"])
    rule_regression = initialize_regression_dict(row)
    if not cur_core_id or cur_core_id == "nan":
        rule_regression["core_id_is_null"] = True
        rule_regression["core_id_startswith_CORE"] = False
        rule_regression["in_cache"] = False
        rule_regression["rule_in_mltple_standards"] = []
        return rule_regression

    rule_regression["core_id_is_null"] = False
    if not cur_core_id.startswith("CORE-"):
        rule_regression["core_id_startswith_CORE"] = False
        rule_regression["in_cache"] = False
        rule_regression["rule_in_mltple_standards"] = []
        return rule_regression

    rule_regression["core_id_startswith_CORE"] = True
    rule = get_core_rule(cur_core_id)
    if not rule or not SQLRuleProcessor.valid_rule_structure(rule):
        rule_regression["in_cache"] = False
        return rule_regression

    rule_regression["in_cache"] = True
    rule_ids = row["rids"]
    for rid in rule_ids:
        paths = get_data_paths_by_rule_id(row, rid)
        if len(paths) == 1:
            rule_regression["rule_in_mltple_standards"] = []
            p = paths[0]
            path_obj = Path(p)
            parts = path_obj.parts
            rule_regression["sharepoint_source"] = parts[-RULE_DEPTH]

            for case in ["negative", "positive"]:
                case_path = path_obj / case
                if case_path.exists():
                    run_test_cases(rule_regression, case, str(case_path), ig_specs, rule, target_case)
        elif len(paths) < 1:
            rule_regression["rule_in_mltple_standards"] = []
        else:
            rule_regression["rule_in_mltple_standards"] = paths
    return rule_regression


def initialize_regression_dict(row) -> dict:
    return {
        "core-id": (row["Core-ID"] if pd.notna(row["Core-ID"]) and str(row["Core-ID"]).strip() else "unknown"),
        "cdisc_rule_id": (
            row["CDISC Rule ID"] if pd.notna(row["CDISC Rule ID"]) and str(row["CDISC Rule ID"]).strip() else "unknown"
        ),
        "standard": (
            row["Standard Name"] if pd.notna(row["Standard Name"]) and str(row["Standard Name"]).strip() else "unknown"
        ),
        "executability": (
            row["Executability"] if pd.notna(row["Executability"]) and str(row["Executability"]).strip() else "unknown"
        ),
        "status": (row["Status"] if pd.notna(row["Status"]) and str(row["Status"]).strip() else "unknown"),
        "standard_source": row["standard_source"],
    }


def run_test_cases(
    cur_regression: dict,
    case: str,
    case_folder_path: str,
    ig_specs: IGSpecification,
    rule,
    target_case: Optional[str] = None,
):
    two_digit_pattern = re.compile(r"^\d{2}$")
    cur_regression[f"{case}_folder_path"] = extract_final_path(case_folder_path, TYPE_DEPTH)
    case_path = Path(case_folder_path)
    test_case_folder_paths = [
        str(case_path / name)
        for name in os.listdir(case_folder_path)
        if (case_path / name).is_dir() and two_digit_pattern.match(name)
    ]

    test_case_regression = []
    for test_case_folder_path in sorted(test_case_folder_paths):
        if target_case and not test_case_folder_path.endswith(target_case):
            continue

        try:
            test_case_path = Path(test_case_folder_path)
            data_path = test_case_path / "data"
            test_case_file_path = find_data_file(str(data_path))
            define_xml_file_path = find_define_xml_file_path(str(data_path))
            # run engine
            engine_regression = {}
            run_regression_on_test_case(
                test_case_folder_path,
                test_case_file_path,
                define_xml_file_path,
                engine_regression,
                ig_specs,
                rule,
            )
            test_case_regression.append(
                {
                    extract_final_path(test_case_folder_path, CASE_DEPTH): {
                        "test_case_xslx_file": extract_final_path(test_case_file_path, DATA_DEPTH),
                        "engine_regression": engine_regression,
                    }
                }
            )
        except FileNotFoundError:
            test_case_regression.append(
                {
                    extract_final_path(test_case_folder_path, CASE_DEPTH): {
                        "test_case_xslx_file": None,
                        "engine_regression": None,
                    }
                }
            )
    cur_regression[f"{case}_regressions"] = test_case_regression


def run_regression_on_test_case(
    test_case_folder_path: str,
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
        process_test_case_dataset(
            regression_errors,
            define_xml_file_path,
            data_test_datasets,
            ig_specs,
            rule,
            test_case_folder_path,
        )

        # Uncomment to produce reports for CDISC to use
        # if regression_errors.get("old_overall_result") == "skipped":
        #     try:
        #         xlsx_data = pd.ExcelFile(data_file_path)
        #         pd.read_excel(xlsx_data, sheet_name="Library")
        #         present = True
        #     except ValueError:
        #         present = False

        #     with open("./skipped.txt", "a") as f:
        #         f.write(
        #             f"""{extract_final_path(test_case_folder_path, 4)} - skipped - Library sheet: {
        #                 "present" if present else "not found"}\n"""
        #         )

    return None, None


def get_metadata(ig_specs: IGSpecification, define_xml_path: str):
    """
    Get metadata from cache or create it if not present.
    """
    key = f"{ig_specs['standard']}_{ig_specs['standard_version']}_{ig_specs['standard_substandard']}_{define_xml_path}"
    if key not in METADATA_CACHE:
        METADATA_CACHE[key] = get_library_metadata_from_cache(
            Validation_args(
                cache=os.path.join(os.path.dirname(__file__), "..", "..", DefaultFilePaths.CACHE.value),
                pool_size=None,
                dataset_paths=None,
                log_level=None,
                report_template=None,
                standard=ig_specs["standard"],
                version=ig_specs["standard_version"],
                substandard=ig_specs["standard_substandard"],
                controlled_terminology_package=None,
                output=None,
                output_format=None,
                raw_report=None,
                define_version=None,
                external_dictionaries=None,
                rules=None,
                local_rules=None,
                custom_standard=None,
                progress=None,
                define_xml_path=define_xml_path,
                validate_xml=None,
            )
        )
    return METADATA_CACHE[key]


def process_test_case_dataset(
    regression_errors: list,
    define_xml_file_path: str,
    data_test_datasets: list,
    ig_specs: IGSpecification,
    rule: dict,
    test_case_folder_path: str,
):
    try:
        # Execute rule in SQL engine
        ds = PostgresQLDataService.from_list_of_testdatasets(data_test_datasets, standard=ig_specs)
        regression_errors["datasets_import_sql"] = "SUCCESS"
        sql_results = sql_run_single_rule_validation(data_service=ds, rule=rule)
        regression_errors["results_present_sql"] = True
        sql_regression = extract_results_regression(sql_results)
        regression_errors["results_sql"] = sql_regression

        # Execute in old engine
        metadata = get_metadata(ig_specs, define_xml_file_path)
        old_results = run_single_rule_validation(
            data_test_datasets,
            rule,
            define_xml=define_xml_file_path,
            standard=ig_specs["standard"],
            standard_version=ig_specs["standard_version"],
            library_metadata=metadata,
        )
        regression_errors["dataset_import_old"] = "SUCCESS"
        regression_errors["results_present_old"] = True
        old_regression = extract_results_regression(old_results)
        regression_errors["results_old"] = old_regression

        regression_errors["old_vs_sql"] = old_vs_sql_regression_comparison(old_regression, sql_regression)

        regression_errors["sql_overall_result"] = extract_overall_result(sql_regression)
        regression_errors["old_overall_result"] = extract_overall_result(old_regression)

        # does validated_results path exist:
        test_case_path = Path(test_case_folder_path)
        validated_results_folder = test_case_path / "validated_results"
        if not validated_results_folder.exists():
            regression_errors["validated_results_folder_exists"] = False
            regression_errors["validation_file"] = ""
            regression_errors["validation_file_validation"] = ""
            regression_errors["old_result_validation"] = "invalid"
            regression_errors["sql_results_validation"] = "invalid"
        else:
            regression_errors["validated_results_folder_exists"] = True
            validation_file_path = find_data_file(str(validated_results_folder))
            if not validation_file_path:
                regression_errors["validation_file"] = ""
                regression_errors["validation_file_validation"] = ""
                regression_errors["old_result_validation"] = "invalid"
                regression_errors["sql_results_validation"] = "invalid"
            else:
                regression_errors["validation_file"] = extract_final_path(validation_file_path, DATA_DEPTH)
                try:
                    with open(validation_file_path, "r", encoding="utf-8") as f:
                        validated_result = json.load(f)
                        regression_errors["validation_file_validation"] = "valid"
                        regression_errors["old_result_validation"] = validate_engine_result(
                            old_regression, validated_result
                        )
                        regression_errors["sql_results_validation"] = validate_engine_result(
                            sql_regression, validated_result
                        )
                except json.decoder.JSONDecodeError as e:
                    regression_errors["validation_file_validation"] = e
                    regression_errors["old_result_validation"] = "invalid"
                    regression_errors["sql_results_validation"] = "invalid"

        return sql_results, old_results

    except ValueError as e:
        if str(e) == "Data list cannot be empty":
            regression_errors["datasets_import_sql"] = f"datasets_dataset_errors: {str(e)}"
        else:
            raise
    except errors.UndefinedColumn as e:
        if "column" in str(e) and "does not exist" in str(e):
            regression_errors["datasets_import_sql"] = f"pre_processor_error: {str(e)}"
        else:
            raise


def validate_engine_result(engine_result: list[dict], validated_result: list[dict]) -> dict:
    val_result = validated_result["results"]
    diff = DeepDiff(engine_result, val_result, ignore_order=True, ignore_string_case=True)
    if diff:
        return "failed"
    else:
        return "valid"


def old_vs_sql_regression_comparison(old_results: list[dict], sql_results: list[dict]):
    dataset_mismatch = False
    execution_status_mismatch = False
    number_of_errors_mismatch = False
    diff = {}
    # compare execution status
    for sql, old in zip(sql_results, old_results):
        if sql.get("dataset") != old.get("dataset") or sql.get("domain") != old.get("domain"):
            dataset_mismatch = True
            break

        if old.get("execution_status") != sql.get("execution_status"):
            execution_status_mismatch = True
            break

        if old.get("number_errors") != sql.get("number_errors"):
            number_of_errors_mismatch = True
            break

        dataset_diff = compare_error_lists(old.get("errors"), sql.get("errors"))
        if dataset_diff:
            diff[old.get("dataset")] = dataset_diff

    if dataset_mismatch or execution_status_mismatch or number_of_errors_mismatch or diff:
        return {
            "dataset_mismatch": dataset_mismatch,
            "execution_status_mismatch": execution_status_mismatch,
            "number_of_errors_mismatch": number_of_errors_mismatch,
            "diff": diff,
        }
    else:
        return {
            "equal": True,
        }


def compare_error_lists(old_errors, sql_errors):
    diff = DeepDiff(old_errors, sql_errors, ignore_order=True, ignore_string_case=True)
    if diff:
        # Calling `to_json` to create a valid JSON (otherwise the output is not JSON serializable)
        # and then converting it back to a Python object so it's formatted properly
        reloaded = json.loads(diff.to_json())
        # Need to sort the values_changed keys for consistent output
        if "values_changed" in reloaded:
            reloaded["values_changed"] = dict(sorted(reloaded["values_changed"].items()))
        return reloaded
    else:
        return []


def extract_results_regression(results):
    res_regression = []

    if isinstance(results, dict):
        result_list = [res[0] for res in results.values()]
    elif isinstance(results, list):
        result_list = results
    else:
        return res_regression

    for res in result_list:
        domain_res_regression = {
            "dataset": res.get("dataset", ""),
            "domain": res.get("domain", ""),
            "execution_status": res.get("executionStatus", ""),
            "execution_message": res.get("message", ""),
            "number_errors": len(res.get("errors", [])),
        }

        execution_status = res.get("executionStatus", "")
        errors = res.get("errors", [])

        if execution_status == "execution_error":
            domain_res_regression["errors"] = [
                {"error": error.get("error"), "message": error.get("message")}
                for error in sorted(errors, key=lambda x: x.get("message", ""))
            ]
        elif execution_status == "skipped":
            domain_res_regression["errors"] = []
        elif execution_status == "success":
            domain_res_regression["errors"] = [
                {
                    "row": error.get("row"),
                    "SEQ": error.get("SEQ"),
                    "USUBJID": error.get("USUBJID"),
                    "value": error.get("value"),
                }
                for error in sorted(errors, key=lambda x: x.get("row", 0))
            ]
        else:
            domain_res_regression["errors"] = [{"error": "unknown execution status"}]
        res_regression.append(domain_res_regression)
    return res_regression


def extract_overall_result(results):
    statuses = [domain["execution_status"] for domain in results]
    if len(statuses) == 0:
        return "missing"

    if "execution_error" in statuses:
        return "execution_error"

    if all(status == "skipped" for status in statuses):
        return "skipped"

    return "success"


def get_data_paths_by_rule_id(row: pd.Series, rid: str) -> list[str]:
    local_path = Path(os.getenv("REGRESSION_PATH"))
    paths = []
    if "SDTMIG" in row["std"]:
        paths.extend(
            find_dirs(
                local_path / "SDTMIG",
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
            local_path / "FDA Business Rules",
            rid,
            case_insensitive=True,
        )
    )
    paths.extend(
        find_dirs(
            local_path / "FDA Validator Rules",
            rid,
            case_insensitive=True,
        )
    )
    return [str(p) for p in paths]


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
                    name=filename.split(".")[0].upper(),
                    label=label,
                    variables=variables,
                    records=data,
                )
            )

    return test_datasets


def extract_variables(
    dataset_df: pd.DataFrame,
) -> Tuple[list[TestVariableMetadata], dict]:
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
            {
                "name": var_name,
                "label": var_label,
                "type": var_type,
                "length": var_length,
                "format": var_format,
            }
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


def find_dirs(root: Path, target_name: str, case_insensitive=False) -> list[Path]:
    matches = []
    if not root.exists():
        return matches

    for item in root.iterdir():
        if item.is_dir():
            if (item.name == target_name) or (case_insensitive and item.name.lower() == target_name.lower()):
                matches.append(item)
    return matches


def find_max_dir(root: str) -> str:
    root_path = Path(root)
    if not root_path.exists():
        return ""

    max_num = 0
    max_dir = ""

    for item in root_path.iterdir():
        if item.is_dir() and item.name.isdigit():
            num = int(item.name)
            if num >= max_num:
                max_num = num
                max_dir = str(item)
    return max_dir


def find_data_file(path: str) -> str:
    if not path:
        return ""
    try:
        # Sorting to remove any non-determinism between OSes
        for filename in sorted(os.listdir(path)):
            full_path = os.path.join(path, filename)
            extension = filename.split(".")[-1].lower()
            if not os.path.isfile(full_path) or extension not in ["xls", "xlsx"]:
                continue

            xlsx_data = pd.ExcelFile(full_path)
            try:
                # these throw an error when the sheet is not present
                # TODO: Should really check for the presence of the library
                # sheet, but nothing runs if it's not present so ¯\_(ツ)_/¯
                # pd.read_excel(xlsx_data, sheet_name="Library")
                pd.read_excel(xlsx_data, sheet_name="Datasets")
            except ValueError:
                continue
            return full_path
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


def output_engine_results_json(pytestconfig, get_core_rules_df, get_core_rule, engine: str):
    rule_id = os.getenv("CURRENT_RULE_DEV", "")
    assert rule_id
    regression_df = get_core_rules_df()
    rule_reg = run_single_rule_regression(regression_df[regression_df["Core-ID"] == rule_id].iloc[0], get_core_rule)
    test_case_results = []
    for test_case in rule_reg["negative_regressions"]:
        key, value = next(iter(test_case.items()))
        results_old = value["engine_regression"].get(f"results_{engine.lower()}", [])
        test_case_results.append({"_".join(key.split("/")[-3:]): {"results": results_old}})
    for test_case in rule_reg["positive_regressions"]:
        key, value = next(iter(test_case.items()))
        results_old = value["engine_regression"].get(f"results_{engine.lower()}", [])
        test_case_results.append({"_".join(key.split("/")[-3:]): {"results": results_old}})

    # output
    output_folder = Path(pytestconfig.rootpath) / f"tests/resources/rules/dev/test_case_results_{engine}/"
    delete_files_in_directory(str(output_folder))
    for result in test_case_results:
        key, value = next(iter(result.items()))
        output_file = output_folder / f"{key}_results.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(value, f, ensure_ascii=False, indent=4)


def extract_final_path(path: str, part_num: int) -> str:
    """Extract the final N parts of a path using pathlib for cross-platform compatibility."""
    if not path:
        return ""

    path_obj = Path(path)
    parts = path_obj.parts

    if len(parts) < part_num:
        raise ValueError(f"Path {path} does not have enough parts to extract {part_num} parts.")

    # Join the last part_num parts using forward slashes for consistency
    return "/".join(parts[-part_num:])


def delete_files_in_directory(dir_path: str):
    """Delete all files in a directory, creating it if it doesn't exist."""
    dir_path_obj = Path(dir_path)

    # Ensure the directory exists before attempting to delete files
    if not dir_path_obj.exists():
        dir_path_obj.mkdir(parents=True, exist_ok=True)

    for file_path in dir_path_obj.iterdir():
        if file_path.is_file():
            file_path.unlink()
