from dataclasses import dataclass

from cdisc_rules_engine.standards.base_dataset_metdata import BaseDatasetMetadata


@dataclass
class SdtmDatasetMetadata2(BaseDatasetMetadata):
    is_supp: bool
    is_split: bool
    rdomain: str
    domain_code: str
