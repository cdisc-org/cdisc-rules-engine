from cdisc_rules_engine.enums.base_enum import BaseEnum


class WhoDrugFormats(BaseEnum):
    C3 = "C3"
    B3 = "B3"


class UniversalWhoDrugFiles(BaseEnum):
    VERSION = "Version"
    CCODE = "CCODE"


class B3WHODrugFiles(BaseEnum):
    MAN = "MAN"
    ING = "ING"
    DDSOURCE = "DDSOURCE"
    DDA = "DDA"
    DD = "DD"
    BNA = "BNA"
    INA = "INA"


class C3WHODrugFiles(BaseEnum):
    UNIT = "UNIT"
    ThG = "ThG"
    SUN = "SUN"
    STR = "STR"
    SRCE = "SRCE"
    PRT = "PRT"
    PRG = "PRG"
    PP = "PP"
    PF = "PF"
    ORG = "ORG"
    MP = "MP"
    ING = "ING"
    ATC = "ATC"
