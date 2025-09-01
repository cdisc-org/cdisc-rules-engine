from typing import Dict, List, TypedDict, Union

from cdisc_rules_engine.models.sql import DATASET_COLUMN_TYPES


class TestVariableMetadata(TypedDict):
    name: str
    label: str
    type: DATASET_COLUMN_TYPES
    length: int
    format: str


class TestDataset(TypedDict):
    filename: str
    filepath: str
    name: str
    label: str
    variables: List[TestVariableMetadata]
    records: Dict[str, List[Union[str, int, float]]]
