from cdisc_rules_engine.enums.base_enum import BaseEnum


class StandardTypes(BaseEnum):
    """Standards supported by CDISC Library; used for CLI validation when not using --custom-standard."""

    SDTMIG = "sdtmig"
    SENDIG = "sendig"
    SENDIG_AR = "sendig-ar"
    SENDIG_DART = "sendig-dart"
    SENDIG_GENETOX = "sendig-genetox"
    ADAMIG = "adamig"
    ADAM_ADAE = "adam-adae"
    ADAM_MD = "adam-md"
    ADAM_NCA = "adam-nca"
    ADAM_OCCDS = "adam-occds"
    ADAM_TTE = "adam-tte"
    ADAM_POPPK = "adam-poppk"
    TIG = "tig"
    USDM = "usdm"
