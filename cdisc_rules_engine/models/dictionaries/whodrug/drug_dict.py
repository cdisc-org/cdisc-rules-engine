from .base_whodrug_term import BaseWhoDrugTerm
from .whodrug_record_types import WhodrugRecordTypes


class DrugDictionary(BaseWhoDrugTerm):
    """
    This class describes the DRUG DICTIONARY (DD) file.
    """

    def __init__(self, record_params: dict):
        super(DrugDictionary, self).__init__(record_params)
        self.drugName: str = record_params["drugName"]
        self.drugSequenceNumber: str = record_params["drugSequenceNumber"]
        self.checkDigit: str = record_params["checkDigit"]

    @classmethod
    def from_txt_line(cls, line: str) -> "DrugDictionary":
        return cls(
            {
                "code": line[:6],  # Drug Record Number
                "drugSequenceNumber": line[6:8],
                "checkDigit": line[11],
                "drugName": line[30:].strip(),
                "type": WhodrugRecordTypes.DRUG_DICT.value,
            }
        )

    def get_identifier(self) -> str:
        return f"{self.drugSequenceNumber}{self.checkDigit}{self.code}"

    def get_parent_identifier(self) -> str:
        return ""
