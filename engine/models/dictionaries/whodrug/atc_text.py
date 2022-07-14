from .whodrug_record_types import WhodrugRecordTypes
from .base_whodrug_term import BaseWhoDrugTerm


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
    def from_txt_line(cls, dictionary_id: str, line: str) -> "AtcText":
        return cls(
            {
                "parentCode": line[:7].strip(),
                "level": int(line[7]),
                "text": line[8:].strip(),
                "type": WhodrugRecordTypes.ATC_TEXT.value,
            }
        )
