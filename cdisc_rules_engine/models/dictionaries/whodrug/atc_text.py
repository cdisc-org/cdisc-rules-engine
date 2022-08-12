from .base_whodrug_term import BaseWhoDrugTerm
from .whodrug_record_types import WhodrugRecordTypes


class AtcText(BaseWhoDrugTerm):
    """
    This class describes the ATC TEXT (INA) file.
    """

    def __init__(self, record_params: dict):
        super(AtcText, self).__init__(record_params)
        self.parentCode: str = record_params["parentCode"]  # ATC Code
        self.level: int = record_params["level"]
        self.text: str = record_params["text"]

    @classmethod
    def from_txt_line(cls, line: str) -> "AtcText":
        parent_code: str = line[:7].strip()
        return cls(
            {
                "parentCode": parent_code,  # ATC Code
                "code": parent_code,  # ATC Code
                "level": int(line[7]),
                "text": line[8:].strip(),
                "type": WhodrugRecordTypes.ATC_TEXT.value,
            }
        )
