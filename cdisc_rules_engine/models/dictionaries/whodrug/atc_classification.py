from .base_whodrug_term import BaseWhoDrugTerm
from .whodrug_record_types import WhodrugRecordTypes


class AtcClassification(BaseWhoDrugTerm):
    """
    This class describes the ATC CLASSIFICATION (DDA) file.
    """

    def __init__(self, record_params: dict):
        super(AtcClassification, self).__init__(record_params)
        self.parentCode: str = record_params["parentCode"]  # Drug Record Number
        self.checkDigit: str = record_params["checkDigit"]
        self.parentSequenceNumber: str = record_params["parentSequenceNumber"]

    @classmethod
    def from_txt_line(cls, line: str) -> "AtcClassification":
        return cls(
            {
                "parentCode": line[:6].strip(),
                "parentSequenceNumber": line[6:8],
                "code": line[12:19].strip(),  # ATC Code
                "checkDigit": line[11].strip(),  # check digit
                "type": WhodrugRecordTypes.ATC_CLASSIFICATION.value,
            }
        )

    def get_identifier(self) -> str:
        return f"{self.checkDigit}{self.code}"

    def get_parent_identifier(self) -> str:
        return f"{self.parentSequenceNumber}{self.checkDigit}{self.parentCode}"
