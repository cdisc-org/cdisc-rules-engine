from os.path import join
from cdisc_rules_engine.enums.base_enum import BaseEnum


class DefaultFilePaths(BaseEnum):
    CACHE = join("resources", "cache")
    EXCEL_TEMPLATE_FILE = join("resources", "templates", "report-template.xlsx")
    USDM_EXCEL_TEMPLATE_FILE = join(
        "resources", "templates", "usdm-report-template.xlsx"
    )
    JSONATA_UTILS = join("resources", "jsonata")
    RULES_CACHE_FILE = "rules.pkl"
    RULES_DICTIONARY = "rules_dictionary.pkl"
    STANDARD_DETAILS_CACHE_FILE = "standards_details.pkl"
    STANDARD_MODELS_CACHE_FILE = "standards_models.pkl"
    VARIABLE_METADATA_CACHE_FILE = "variables_metadata.pkl"
    VARIABLE_CODELIST_CACHE_FILE = "variable_codelist_maps.pkl"
    CODELIST_TERM_MAP_CACHE_FILE = "codelist_term_maps.pkl"
    CUSTOM_RULES_CACHE_FILE = "custom_rules.pkl"
    CUSTOM_RULES_DICTIONARY = "custom_rules_dictionary.pkl"
    LOCAL_XSD_FILE_DIR = join("resources", "schema", "xml")
    LOCAL_XSD_FILE_MAP = {
        "http://www.cdisc.org/ns/usdm/xhtml/v1.0": join(
            "cdisc-usdm-xhtml-1.0", "usdm-xhtml-1.0.xsd"
        ),
        "http://www.w3.org/1999/xhtml": join("xhtml-1.1", "xhtml11.xsd"),
    }
