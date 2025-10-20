from dataclasses import dataclass
from typing import Dict, List, Union

from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2


@dataclass
class TestDataset(DatasetMetadata2):
    records: Dict[str, List[Union[str, int, float]]]
