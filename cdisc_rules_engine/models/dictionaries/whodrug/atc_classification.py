from .base_whodrug_term import BaseWhoDrugTerm
from .whodrug_record_types import WhodrugRecordTypes


class AtcClassification(BaseWhoDrugTerm):
    """
    This class describes the ATC CLASSIFICATION (DDA) file.
    """

    def __init__(self, record_params: dict):
        super(AtcClassification, self).__init__(record_params)
        self.parentCode: str = record_params["parentCode"]  # Drug Record Number

    @classmethod
    def from_txt_line(cls, line: str) -> "AtcClassification":
        return cls(
            {
                "parentCode": line[:6].strip(),
                "code": line[12:19].strip(),  # ATC Code
                "type": WhodrugRecordTypes.ATC_CLASSIFICATION.value,
            }
        )
