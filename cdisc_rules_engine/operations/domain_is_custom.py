from cdisc_rules_engine.operations.base_operation import BaseOperation


class DomainIsCustom(BaseOperation):
    def _execute_operation(self):
        """
        Gets standard details from cache and checks if
        given domain is in standard domains.
        If no -> the domain is custom.
        """
        return self.library_metadata.is_domain_custom(self.params.domain)
