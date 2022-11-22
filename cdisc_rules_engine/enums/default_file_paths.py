from cdisc_rules_engine.enums.base_enum import BaseEnum


class DefaultFilePaths(BaseEnum):
    CACHE = "resources/cache"
    EXCEL_TEMPLATE_FILE = "resources/templates/report-template.xlsx"
    RULES_CACHE_FILE = "rules.pkl"
    STANDARD_DETAILS_CACHE_FILE = "standards_details.pkl"
    STANDARD_MODELS_CACHE_FILE = "standards_models.pkl"
    VARIABLE_METADATA_CACHE_FILE = "variables_metadata.pkl"
    VARIABLE_CODELIST_CACHE_FILE = "variable_codelist_maps.pkl"
    CODELIST_TERM_MAP_CACHE_FILE = "codelist_term_maps.pkl"
