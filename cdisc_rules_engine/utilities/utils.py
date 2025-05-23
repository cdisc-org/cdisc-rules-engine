"""
This module contains utility functions
that can be reused.
"""

import copy
import os
import re
import pandas as pd
from datetime import datetime
from typing import Callable, Iterable, List, Optional, Union
from uuid import UUID
from cdisc_rules_engine.constants.metadata_columns import (
    SOURCE_FILENAME,
    SOURCE_ROW_NUMBER,
)
from cdisc_rules_engine.models.dataset.dataset_interface import DatasetInterface
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata

from cdisc_rules_engine.constants.domains import (
    AP_DOMAIN,
    APFA_DOMAIN,
    APRELSUB_DOMAIN,
    SUPPLEMENTARY_DOMAINS,
)
from cdisc_rules_engine.constants.classes import SPECIAL_PURPOSE
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.interfaces import ConditionInterface
from cdisc_rules_engine.models.base_validation_entity import BaseValidationEntity
from business_rules.utils import is_valid_date
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata


def convert_file_size(size_in_bytes: int, desired_unit: str) -> float:
    """
    Converts file size from bytes to any of the following units:
    KB, MB, GB
    """
    unit_to_denominator_map: dict = {
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
    }
    return size_in_bytes / unit_to_denominator_map[desired_unit]


def get_execution_status(results):
    """
    If all results have skipped status, return skipped.
    Else return success
    """
    if len(results) == 0:
        return ExecutionStatus.SUCCESS.value
    if isinstance(results[0], BaseValidationEntity):
        successful_results = [
            entity for entity in results if entity.status == ExecutionStatus.SUCCESS
        ]
    else:
        successful_results = [
            result
            for result in results
            if result.get("executionStatus") == ExecutionStatus.SUCCESS.value
        ]
    if successful_results:
        return ExecutionStatus.SUCCESS.value
    else:
        return ExecutionStatus.SKIPPED.value


def get_standard_codelist_cache_key(standard: str, version: str) -> str:
    return f"{standard.lower()}-{version.replace('.', '-')}-codelists"


def is_valid_iso_date(date_to_validate: str) -> bool:
    """
    Validates a given date against an ISO Format.
    Valid date example: 2022-02-04T15:29:20.173854
    """
    is_valid = True
    try:
        datetime.fromisoformat(date_to_validate)
    except ValueError:
        is_valid = False
    return is_valid


def get_dataset_path(
    study_id: str, data_bundle_id: str = None, filename: str = None
) -> str:
    """
    Returns a path to dataset in the blob storage.
    """
    path: str = study_id
    if data_bundle_id:
        path = os.path.join(path, data_bundle_id)
    if filename:
        path = os.path.join(path, filename)
    return path


DATASET_CACHE_KEY_TEMPLATE: str = "{dataset_path}_{dataset_type}"


def get_dataset_cache_key_from_study(
    study_id: str,
    data_bundle_id: str = None,
    filename: str = None,
    dataset_type: str = None,
) -> str:
    """
    Creates a cache key for a dataset.
    Usually, template of a dataset cache key is {dataset_path}_{dataset_type}.
    Ex.: CDISC01/test/ae.xpt_contents or CDISC01/test/ae.xpt_metadata.
    So, the function also builds the path.

    If dataset_type parameter is not passed, the returned key
    can be used to clean several values with matching key pattern.
    dataset_type param can be: contents, metadata, variables_metadata.
    """
    dataset_path: str = get_dataset_path(study_id, data_bundle_id, filename)
    if dataset_type:
        dataset_path = DATASET_CACHE_KEY_TEMPLATE.format(
            dataset_path=dataset_path, dataset_type=dataset_type
        )
    return dataset_path


def get_dataset_cache_key_from_path(dataset_path: str, dataset_type: str) -> str:
    return DATASET_CACHE_KEY_TEMPLATE.format(
        dataset_path=dataset_path, dataset_type=dataset_type
    )


def is_supp_domain(dataset_domain: str) -> bool:
    """
    Returns true if domain name starts with SUPP or SQ
    """
    return dataset_domain.startswith(SUPPLEMENTARY_DOMAINS)


def is_ap_domain(dataset_domain: str) -> bool:
    """
    Returns true if domain name is like AP-- / APFA APRELSUB.
    """
    if dataset_domain == APRELSUB_DOMAIN:
        return True
    if len(dataset_domain) == 6:
        domain_to_check: str = APFA_DOMAIN
    else:
        domain_to_check: str = AP_DOMAIN
    regex = r"^" + re.escape(domain_to_check) + "[a-zA-Z]{2,4}$"
    return bool(re.match(regex, dataset_domain))


def get_library_variables_metadata_cache_key(
    standard_type: str, standard_version: str, standard_substandard: str
) -> str:
    if not standard_substandard:
        return f"library_variables_metadata/{standard_type}/{standard_version}"
    else:
        return f"library_variables_metadata/{standard_type}/{standard_version}/{standard_substandard}"


def get_standard_details_cache_key(
    standard_type: str, standard_version: str, standard_substandard: str = None
) -> str:
    if not standard_substandard:
        return f"standards/{standard_type}/{standard_version}"
    else:
        return f"standards/{standard_type}/{standard_version}/{standard_substandard}"


def get_model_details_cache_key(standard: str, model_version: str) -> str:
    return f"models/{standard}/{model_version.replace('.', '-')}"


def get_model_details_cache_key_from_ig(standard_metadata: dict) -> str:
    model_link = standard_metadata.get("_links", {}).get("model", {}).get("href", "")
    model_link_parts = model_link.split("/")
    return get_model_details_cache_key(
        standard=model_link_parts[2], model_version=model_link_parts[3]
    )


def replace_pattern_in_list_of_strings(
    list_of_strings: List[str], pattern: str, value: str
) -> List[str]:
    return [string.replace(pattern, value or "") for string in list_of_strings]


def get_operations_cache_key(
    core_id: str,
    directory_path: str,
    operation_id: str,
    domain: str = None,
    operation_name: str = None,
    grouping: str = None,
    target_variable: str = None,
    dataset_path: str = None,
) -> str:
    """
    Creates the cache key for operations.
    """
    key = f"operations/{core_id}/{directory_path}/{operation_id}"
    optional_items = [domain, operation_name, grouping, target_variable, dataset_path]
    for item in optional_items:
        if item:
            key = f"{key}/{item}"
    return key


def get_directory_path(dataset_path):
    return os.path.dirname(dataset_path)


def tag_source(
    dataset: DatasetInterface, dataset_metadata: DatasetMetadata
) -> DatasetInterface:
    """
    For sdtm split datasets,
    Adds source filename and row number to dataset
    """
    dataset[SOURCE_FILENAME] = dataset_metadata.filename
    dataset[SOURCE_ROW_NUMBER] = list(range(1, dataset.len() + 1))
    return dataset


def get_corresponding_datasets(
    datasets: Iterable[SDTMDatasetMetadata], dataset_metadata: SDTMDatasetMetadata
) -> List[SDTMDatasetMetadata]:
    return [
        other
        for other in datasets
        if dataset_metadata.unsplit_name == other.unsplit_name
    ]


def get_dataset_name_from_details(dataset_metadata: SDTMDatasetMetadata) -> str:
    return (
        os.path.split(dataset_metadata.full_path)[-1]
        if dataset_metadata.full_path
        else dataset_metadata.filename
    )


def serialize_rule(rule: dict) -> dict:
    """
    Converts rule "conditions" to dict.
    TODO create a Rule class and move this function there
    """
    serialized_rule: dict = copy.deepcopy(rule)
    conditions: ConditionInterface = serialized_rule["conditions"]
    serialized_rule["conditions"] = conditions.to_dict()
    return serialized_rule


def get_cache_last_updated_key() -> str:
    return "CACHE_LAST_UPDATED"


def remove_none_keys_from_dict(dict_to_remove: dict):
    """
    Removes dict keys whose value is None.
    Changes the dict by reference.
    """
    # dict can't change its size during iteration
    dict_copy: dict = copy.deepcopy(dict_to_remove)
    for key, value in dict_copy.items():
        if value is None:
            dict_to_remove.pop(key)


def list_contains_duplicates(list_to_check: list) -> bool:
    """
    Checks if a list contains duplicated items.
    """
    return bool(len(list_to_check) > len(set(list_to_check)))


def extract_file_name_from_path_string(path: str) -> str:
    """
    Extracts file name from given path string.
    Example:
        input: "CDISC01/test/ae.xpt"
        output: ae.xpt
    """
    return os.path.split(path)[-1]


def generate_report_filename(generation_time: str) -> str:
    timestamp = (
        datetime.fromisoformat(generation_time)
        .replace(microsecond=0)
        .isoformat()
        .replace(":", "-")
    )
    return f"CORE-Report-{timestamp}"


def get_rules_cache_key(standard: str, version: str, substandard: str = None) -> str:
    if substandard:
        key = f"{standard.lower()}/{version}/{substandard.lower()}"
    else:
        key = f"{standard.lower()}/{version}"
    return key


def get_metadata_cache_key(metadata_key: str):
    return f"library/metadata{metadata_key}"


def get_variable_codelist_map_cache_key(standard: str, version: str, subversion) -> str:
    if subversion:
        return f"{standard}-{version}-{subversion}-codelists"
    else:
        return f"{standard}-{version}-codelists"


def get_meddra_code_term_pairs_cache_key(meddra_path: str) -> str:
    return f"meddra_valid_code_term_pairs_{meddra_path}"


def get_item_index_by_condition(
    list_of_dicts: List[dict], condition: Callable
) -> Optional[int]:
    """
    Uses linear search to return index of element
    in unsorted list which applies to the condition.
    """
    for index, dictionary in enumerate(list_of_dicts):
        if condition(dictionary):
            return index


def search_in_list_of_dicts(
    list_of_dicts: List[dict], condition: Callable
) -> Optional[dict]:
    """
    Returns an element of unsorted list that applies to the condition.
    """
    index = get_item_index_by_condition(list_of_dicts, condition)
    if index is not None:
        return list_of_dicts[index]


def is_valid_uuid(string_to_validate: str) -> bool:
    """
    Checks if a given string is a valid UUID.
    """
    try:
        UUID(string_to_validate)
    except ValueError:
        return False
    return True


def get_dictionary_path(directory_path: str, file_name: str) -> str:
    """
    Creates a path to dictionary directory or file.
    """
    return os.path.join(directory_path, file_name)


def convert_library_class_name_to_ct_class(class_name: str):
    conversions = {"special-purpose": SPECIAL_PURPOSE}
    return conversions.get(class_name.lower(), class_name.upper())


def decode_line(line: bytes) -> str:
    return line.decode("utf-8").replace("\n", "").replace("\r", "")


def get_sided_match_keys(match_keys: List[Union[str, dict]], side: str) -> List[str]:
    return [
        match_key if isinstance(match_key, str) else match_key[side]
        for match_key in match_keys
    ]


def parse_date(date_str):
    if not isinstance(date_str, str) or not is_valid_date(date_str):
        return 0, 0
    if "--" in date_str:
        date_str = date_str.split("--", 1)[0]
    parts = re.split(r"[-T:]", date_str)
    precision = len([part for part in parts if part])
    return date_str, precision


def dates_overlap(date1_str, precision1, date2_str, precision2):
    if precision1 == precision2:
        return date1_str == date2_str, None

    less_precise = date1_str if precision1 < precision2 else date2_str
    more_precise = date2_str if precision1 < precision2 else date1_str

    return more_precise.startswith(less_precise), less_precise


def replace_nan_values_in_df(df, columns):
    for col in columns:
        if col in df.columns:
            mask = pd.isna(df[col])
            if mask.any():
                df.loc[mask, col] = None
    return df
