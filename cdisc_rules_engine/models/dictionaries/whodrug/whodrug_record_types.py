from cdisc_rules_engine.enums.base_enum import BaseEnum


class WhodrugRecordTypes(BaseEnum):
    """
    This enum holds available whodrug record types.
    """

    DRUG_DICT = "DRUG_DICT"
    ATC_TEXT = "ATC_TEXT"
    ATC_CLASSIFICATION = "ATC_CLASSIFICATION"
