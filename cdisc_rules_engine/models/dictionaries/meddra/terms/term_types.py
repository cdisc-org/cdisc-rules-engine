from cdisc_rules_engine.enums.base_enum import BaseEnum


class TermTypes(BaseEnum):
    """
    This enum holds all available meddra term types.
    """

    SOC = "soc"
    LLT = "llt"
    HLGT = "hlgt"
    PT = "pt"
    HLT = "hlt"
