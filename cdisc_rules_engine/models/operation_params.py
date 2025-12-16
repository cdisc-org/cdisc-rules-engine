from dataclasses import dataclass
from typing import Iterable, List
from cdisc_rules_engine.models.external_dictionaries_container import (
    ExternalDictionariesContainer,
)
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata

import pandas as pd


@dataclass
class OperationParams:
    """
    This class defines input parameters for rule operations.
    Rule operations are defined in DataProcessor class.
    """

    # Required parameters (no defaults) first
    core_id: str
    dataframe: pd.DataFrame
    dataset_path: str
    datasets: Iterable[SDTMDatasetMetadata]
    domain: str
    directory_path: str
    operation_id: str
    operation_name: str
    standard: str
    standard_version: str

    # Optional parameters with defaults
    standard_substandard: str = None
    attribute_name: str = None
    case_sensitive: bool = True
    codelist: str = None
    codelist_code: str = None
    codelists: list = None
    ct_attribute: str = None
    ct_package_types: List[str] = None
    ct_version: str = None
    ct_package_type: str = None
    term_code: str = None
    term_value: str = None
    term_pref_term: str = None
    dictionary_term_type: str = None
    external_dictionaries: ExternalDictionariesContainer = None
    external_dictionary_term_variable: str = None
    external_dictionary_type: str = None
    filter: dict = None
    grouping: List[str] = None
    grouping_aliases: List[str] = None
    key_name: str = None
    key_value: str = None
    level: str = None
    map: List[dict] = None
    original_target: str = None
    regex: str = None
    returntype: str = None
    target: str = None
    value_is_reference: bool = False
    namespace: str = None
    delimiter: str = None
