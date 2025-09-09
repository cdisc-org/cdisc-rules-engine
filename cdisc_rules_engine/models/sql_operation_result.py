from dataclasses import dataclass
from typing import Literal

from cdisc_rules_engine.models.sql import DATASET_COLUMN_TYPES


@dataclass
class SqlOperationResult:
    """
    This class stores the output of a SQL operation.
    """

    query: str
    type: Literal["collection", "constant", "table"]
    subtype: DATASET_COLUMN_TYPES
