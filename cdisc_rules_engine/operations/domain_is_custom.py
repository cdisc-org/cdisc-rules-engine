from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.utilities.utils import (
    get_standard_details_cache_key,
)
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService
from cdisc_rules_engine import config


class DomainIsCustom(BaseOperation):
    def _execute_operation(self):
        """
        Gets standard details from cache and checks if
        given domain is in standard domains.
        If no -> the domain is custom.
        """
        cache_key = get_standard_details_cache_key(
            self.params.standard, self.params.standard_version
        )
        standard_data: dict = self.cache.get(cache_key)
        if standard_data is None:
            cdisc_library_service = CDISCLibraryService(config, self.cache)
            standard_data = cdisc_library_service.get_standard_details(
                self.params.standard.lower(), self.params.standard_version
            )
            self.cache.add(cache_key, standard_data)
        return self.params.domain not in standard_data.get("domains", {})
