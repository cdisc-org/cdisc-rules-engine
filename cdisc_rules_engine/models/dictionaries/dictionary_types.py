from cdisc_rules_engine.enums.base_enum import BaseEnum


class DictionaryTypes(BaseEnum):
    """
    This enum holds all available dictionary types.
    """

    MEDDRA = "meddra"
    WHODRUG = "whodrug"
    LOINC = "loinc"
    MEDRT = "medrt"
    UNII = "unii"
    SNOMED = "snomed"
