from cdisc_rules_engine.enums.base_enum import BaseEnum


class StandardTypes(BaseEnum):
    """Standards supported by CDISC Library; used for CLI validation when not using --custom-standard."""

    SDTMIG = "sdtmig"
    SENDIG = "sendig"
    ADAM = "adam"
    CDASHIG = "cdashig"
    TIG = "tig"
