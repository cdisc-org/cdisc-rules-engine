from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.utilities.utils import (
    get_standard_details_cache_key,
    search_in_list_of_dicts,
)
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService
from cdisc_rules_engine import config


class DomainLabel(BaseOperation):
    def _execute_operation(self):
        """
        Return the domain label for the currently executing domain
        """
        cache_key = get_standard_details_cache_key(
            self.params.standard, self.params.standard_version
        )
        standard_data: dict = self.cache.get(cache_key)
        if standard_data is None:
            cdisc_library_service = CDISCLibraryService(config, self.cache)
            standard_data = set(
                cdisc_library_service.get_standard(
                    self.params.standard.lower(), self.params.standard_version
                )
            )
            self.cache.set(cache_key, standard_data)
        domain_details = None
        for c in standard_data.get("classes", []):
            domain_details = search_in_list_of_dicts(
                c.get("datasets", []), lambda item: item["name"] == self.params.domain
            )
            if domain_details:
                return domain_details.get("label", "")
        return ""
