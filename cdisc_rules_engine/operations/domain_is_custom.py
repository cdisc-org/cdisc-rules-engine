from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService
from cdisc_rules_engine import config


class DomainIsCustom(BaseOperation):
    def _execute_operation(self):
        """
        Gets standard details from cache and checks if
        given domain is in standard domains.
        If no -> the domain is custom.
        """
        standard_data: dict = self.library_metadata.standard_metadata
        if not standard_data:
            cdisc_library_service = CDISCLibraryService(config, self.cache)
            standard_data = cdisc_library_service.get_standard_details(
                self.params.standard.lower(),
                self.params.standard_version,
                self.params.substandard,
            )
            self.library_metadata.standard_metadata = standard_data
        return self.params.domain not in standard_data.get("domains", {})
