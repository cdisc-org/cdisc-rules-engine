from dataclasses import dataclass
from typing import Dict, Literal, Optional

from cdisc_rules_engine.models.sql import DATASET_COLUMN_TYPES


@dataclass
class SqlOperationResult:
    """
    This class stores the output of a SQL operation.
    """

    query: str
    type: Literal["collection", "constant"]
    subtype: DATASET_COLUMN_TYPES
    params: Optional[Dict[str, str]] = None
