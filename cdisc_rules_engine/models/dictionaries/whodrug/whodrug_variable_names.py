from cdisc_rules_engine.enums.base_enum import BaseEnum


class WhodrugVariableNames(BaseEnum):
    """
    This enum holds available whodrug record types.
    """

    DRUG_NAME = "DECOD"
    ATC_TEXT = "CLAS"
    ATC_CLASSIFICATION = "CLASCD"
