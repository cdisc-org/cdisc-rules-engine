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
    target: str = None
    meddra_path: str = None
    whodrug_path: str = None
    grouping: List[str] = None
