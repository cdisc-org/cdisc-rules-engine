from cdisc_rules_engine.enums.base_enum import BaseEnum
from typing import List


class LibraryEndpoints(BaseEnum):
    PRODUCTS = "/mdr/products"
    RULES = "/mdr/rules"
    PACKAGES = "/mdr/ct/packages"
    DATA_ANALYSIS = "/mdr/products/DataAnalysis"
    DATA_COLLECTION = "/mdr/products/DataCollection"
    DATA_TABULATION = "/mdr/products/DataTabulation"
    TIG = "/mdr/integrated/tig/1-0;"

    @classmethod
    def get_tig_options(cls) -> List[str]:
        """
        Returns a list of complete TIG endpoint paths for all versions.
        """
        base_path = "/mdr/integrated/tig"
        versions = [
            v.strip() for v in cls.TIG.value.split("/")[-1].split(";") if v.strip()
        ]
        endpoints = []
        for version in versions:
            endpoints.append(f"{base_path}/{version}")
        return endpoints


def get_tig_endpoints() -> List[str]:
    return LibraryEndpoints.get_tig_options()
