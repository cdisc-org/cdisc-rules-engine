from dataclasses import dataclass
from typing import List

from cdisc_rules_engine.models.sql import DATASET_COLUMN_TYPES


@dataclass
class VariableMetadata:
    name: str
    label: str
    type: DATASET_COLUMN_TYPES
    length: int
    format: str
    order: int


@dataclass
class DatasetMetadata2:
    filename: str
    name: str
    label: str
    variables: List[VariableMetadata]
