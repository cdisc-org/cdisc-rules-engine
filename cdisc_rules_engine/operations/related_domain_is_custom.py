from cdisc_rules_engine.operations.base_operation import BaseOperation


class RelatedDomainIsCustom(BaseOperation):
    def _execute_operation(self):
        """
        Gets standard details from cache and checks if
        given domain is in standard domains.
        If no -> the domain is custom.
        """
        standard_data: dict = self.library_metadata.standard_metadata

        for ds in self.params.datasets:
            if ds.is_supp and self.params.domain.endswith(ds.rdomain):
                return ds.rdomain not in standard_data.get("domains", {})
        return False
