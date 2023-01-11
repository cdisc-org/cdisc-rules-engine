from dataclasses import dataclass
from typing import Union


@dataclass
class DatasetMetadata:
    """
    This class is a container for dataset metadata
    """

    name: str
    domain_name: str
    label: str
    modification_date: str
    filename: str
    size: Union[int, float]
    records: str
    full_path: str = None
