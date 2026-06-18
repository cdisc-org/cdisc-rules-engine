from cdisc_rules_engine.enums.base_enum import BaseEnum


class StaticTables(BaseEnum):
    IG_CODELIST_TABLE_NAME = "ig_codelists"
    IG_DATASETS_TABLE_NAME = "ig_datasets"
    IG_VARIABLES_TABLE_NAME = "ig_variables"
    CG_TAUGS_TABLE_NAME = "cg_taugs"
    WHODRUG_TABLE_NAME = "ex_whodrug"
    MEDDRA_TABLE_NAME = "ex_meddra"
    UNII_TABLE_NAME = "ex_unii"
    MEDRT_TABLE_NAME = "ex_medrt"
    LOINC_TABLE_NAME = "ex_loinc"
    SNOMED_TABLE_NAME = "ex_snomed"
