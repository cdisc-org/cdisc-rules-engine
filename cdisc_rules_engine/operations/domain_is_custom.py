from cdisc_rules_engine.operations.base_operation import BaseOperation


class DomainIsCustom(BaseOperation):
    def _execute_operation(self):
        """
        Gets standard details from cache and checks if
        given domain is in standard domains.
        If no -> the domain is custom.
        """
        standard_data: dict = self.library_metadata.standard_metadata
        return self.params.domain not in standard_data.get("domains", {})
