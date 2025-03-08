from dataclasses import dataclass
from typing import Union
from cdisc_rules_engine.constants.domains import SUPPLEMENTARY_DOMAINS
from cdisc_rules_engine.models.dataset_metadata import DatasetMetadata


@dataclass
class SDTMDatasetMetadata(DatasetMetadata):
    """
    This class is a container for SDTM dataset metadata
    """

    """
    Examples
    | name     | unsplit_name | is_supp | domain | rdomain |
    | -------- | ------------ | ------- | ------ | ------- |
    | QS       | QS           | False   | QS     | None    |
    | QSX      | QS           | False   | QS     | None    |
    | QSXX     | QS           | False   | QS     | None    |
    | SUPPQS   | SUPPQS       | True    | None   | QS      |
    | SUPPQSX  | SUPPQS       | True    | None   | QS      |
    | SUPPQSXX | SUPPQS       | True    | None   | QS      |
    | APQS     | APQS         | False   | APQS   | None    |
    | APQSX    | APQS         | False   | APQS   | None    |
    | APQSXX   | APQS         | False   | APQS   | None    |
    | SQAPQS   | SQAPQS       | True    | None   | APQS    |
    | SQAPQSX  | SQAPQS       | True    | None   | APQS    |
    | SQAPQSXX | SQAPQS       | True    | None   | APQS    |
    | RELREC   | RELREC       | False   | None   | None    |
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
        if self.domain:
            return self.domain
        if self.name.startswith("SUPP"):
            return f"SUPP{self.rdomain}"
        if self.name.startswith("SQ"):
            return f"SQ{self.rdomain}"
        return self.name

    @property
    def is_split(self) -> bool:
        return self.name != self.unsplit_name
