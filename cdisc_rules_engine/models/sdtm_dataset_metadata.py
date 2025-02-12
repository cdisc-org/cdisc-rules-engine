from dataclasses import dataclass
from typing import Union
from cdisc_rules_engine.constants.domains import SUPPLEMENTARY_DOMAINS
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata


@dataclass
class SDTMDatasetMetadata(DatasetMetadata):
    """
    This class is a container for SDTM dataset metadata
    """

    @property
    def domain(self) -> Union[str, None]:
        return (self.first_record or {}).get("DOMAIN", None)

    @property
    def rdomain(self) -> Union[str, None]:
        return (self.first_record or {}).get("RDOMAIN", None) if self.is_supp else None

    @property
    def is_supp(self) -> bool:
        """
        Returns true if name starts with SUPP or SQ
        """
        return self.name.startswith(SUPPLEMENTARY_DOMAINS)

    @property
    def unsplit_name(self) -> str:
        return (
            self.name[:-2]
            if (self.domain and self.name[:-2] == self.domain)
            or (
                self.is_supp
                and (
                    self.name[:-2] == f"SUPP{self.rdomain}"
                    or self.name[:-2] == f"SQ{self.rdomain}"
                )
            )
            else self.name
        )
