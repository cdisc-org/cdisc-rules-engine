from cdisc_rules_engine.enums.base_enum import BaseEnum


class LibraryEndpoints(BaseEnum):
    PRODUCTS = "/mdr/products"
    RULES = "/mdr/rules"
    PACKAGES = "/mdr/ct/packages"
    DATA_ANALYSIS = "/mdr/products/DataAnalysis"
    DATA_COLLECTION = "/mdr/products/DataCollection"
    DATA_TABULATION = "/mdr/products/DataTabulation"
