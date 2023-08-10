from dataclasses import dataclass
from typing import List

import pandas as pd


@dataclass
class OperationParams:
    """
    This class defines input parameters for rule operations.
    Rule operations are defined in DataProcessor class.
    """

    operation_id: str
    operation_name: str
    dataframe: pd.DataFrame
    domain: str
    dataset_path: str
    directory_path: str
    datasets: List[dict]
    standard: str
    standard_version: str
    ct_package: list = None
    ct_packages: list = None
    ct_attribute: str = None
    ct_version: str = None
    target: str = None
    original_target: str = None
    meddra_path: str = None
    whodrug_path: str = None
    grouping: List[str] = None
    key_name: str = None
    key_value: str = None
    attribute_name: str = None
    external_dictionary_type: str = None
    dictionary_term_type: str = None
    case_sensitive: bool = True
