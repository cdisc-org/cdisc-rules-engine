from typing import TypedDict, Optional


class IGSpecification(TypedDict):
    standard: str
    standard_version: str
    standard_substandard: Optional[str]
    define_xml_version: Optional[str]
