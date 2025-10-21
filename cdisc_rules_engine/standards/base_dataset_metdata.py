from dataclasses import dataclass

from cdisc_rules_engine.models.dataset_metadata2 import DatasetMetadata2


@dataclass
class BaseDatasetMetadata(DatasetMetadata2):
    domain: str
