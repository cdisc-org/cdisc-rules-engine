from cdisc_rules_engine.utilities.utils import (
    search_in_list_of_dicts,
)
from typing import Tuple


def get_class_and_domain_metadata(standard_details, domain: str) -> Tuple:
    # Get domain and class details for domain. This logic is specific
    # to SDTM based standards. Needs to be expanded for other models
    for c in standard_details.get("classes", []):
        domain_details = search_in_list_of_dicts(
            c.get("datasets", []), lambda item: item["name"] == domain
        )
        if domain_details:
            return c, domain_details
    return {}, {}
