from dataclasses import dataclass


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
    size: str
    records: str
