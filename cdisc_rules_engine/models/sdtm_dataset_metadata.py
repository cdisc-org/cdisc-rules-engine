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
    | name     | unsplit_name | is_supp | domain | rdomain | is_ap | ap_suffix | dataset_is_custom | related_domain	| related_domain_is_custom |
    | -------- | ------------ | ------- | ------ | ------- | ----- | --------- | ----------------- | -------------- | ------------------------ |
    | QS       | QS           | False   | QS     | None    | False |           | False             |                |                          |
    | QSX      | QS           | False   | QS     | None    | False |           | False             |                |                          |
    | QSXX     | QS           | False   | QS     | None    | False |           | False             |                |                          |
    | SUPPQS   | SUPPQS       | True    | None   | QS      | False |           | False             | QS             |                          |
    | SUPPQSX  | SUPPQS       | True    | None   | QS      | False |           | False             | QS             |                          |
    | SUPPQSXX | SUPPQS       | True    | None   | QS      | False |           | False             | QS             |                          |
    | APQS     | APQS         | False   | APQS   | None    | True  | QS        | False             | QS             |                          |
    | APQSX    | APQS         | False   | APQS   | None    | True  | QS        | False             | QS             |                          |
    | APQSXX   | APQS         | False   | APQS   | None    | True  | QS        | False             | QS             |                          |
    | SQAPQS   | SQAPQS       | True    | None   | APQS    | True  |           | False             | QS             |                          |
    | SQAPQSX  | SQAPQS       | True    | None   | APQS    | True  |           | False             | QS             |                          |
    | SQAPQSXX | SQAPQS       | True    | None   | APQS    | True  |           | False             |                |                          |
    | RELREC   | RELREC       | False   | None   | None    | False |           | False             |                |                          |
    | XX	   | XX	          | False	| XX	 | None    | False |           | True              |                |                          |		
    | SUPPXX   | SUPPXX	      | True	| None   | XX	   | False |           | False             | XX             | True                     |
    | APXX	   | APXX	      | False	| APXX	 | None    | True  | XX	       | False	           | XX             | True                     |
    | SQAPXX   | SQAPXX	      | True	| None   | APXX	   | True  |     	   | False             | XX	            | True                     |
    | FA	   | FA	          | False	| FA	 | None    | False |           | False             |                |                          |
    """  # noqa: E501 W291

    @property
    def domain(self) -> Union[str, None]:
        return (self.first_record or {}).get("DOMAIN", None)

    @property
    def domain_cleaned(self) -> Union[str, None]:
        return self.domain.replace("AP", "") if self.domain else None

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

    @property
    def is_ap(self) -> bool:
        """
        Returns true if APID variable exists in first_record for non-supp datasets,
        or if rdomain is exactly 4 characters and starts with AP for supp datasets.
        """
        if self.is_supp:
            return (
                isinstance(self.rdomain, str)
                and len(self.rdomain) == 4
                and self.rdomain.startswith("AP")
            )
        first_record = self.first_record or {}
        return "APID" in first_record

    @property
    def ap_suffix(self) -> str:
        """
        Returns the 2-character suffix (characters 3-4) from AP domains.
        Returns empty string if not an AP domain or for supp datasets.
        """
        if not self.is_ap:
            return ""
        if self.is_supp:
            return ""
        if isinstance(self.domain, str) and len(self.domain) >= 4:
            return self.domain[2::]
        return ""
