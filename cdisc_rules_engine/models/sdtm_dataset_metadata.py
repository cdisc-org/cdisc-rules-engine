from dataclasses import dataclass
from typing import Union
from cdisc_rules_engine.constants.domains import SUPPLEMENTARY_DOMAINS
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata


@dataclass
class SDTMDatasetMetadata(DatasetMetadata):
    """
    This class is a container for SDTM dataset metadata
    """

    domain: Union[str, None] = None
    rdomain: Union[str, None] = None

    def is_supp(self) -> bool:
        """
        Returns true if domain name starts with SUPP or SQ
        """
        return self.name.startswith(SUPPLEMENTARY_DOMAINS)
