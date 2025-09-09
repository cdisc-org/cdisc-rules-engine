from dataclasses import dataclass
from typing import Literal


@dataclass
class SqlOperationResult:
    """
    This class stores the output of a SQL operation.
    """

    query: str
    type: Literal["collection", "constant", "table"]
