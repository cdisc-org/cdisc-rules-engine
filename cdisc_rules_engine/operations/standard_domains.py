from cdisc_rules_engine.operations.base_operation import BaseOperation


class StandardDomains(BaseOperation):
    def _execute_operation(self):
        standard_data: dict = self.library_metadata.standard_metadata
        domains = standard_data.get("domains", set())
        if isinstance(domains, (set, list, tuple)):
            return sorted(list(domains))
        elif domains is None:
            return []
        raise TypeError(
            f"Invalid type for 'domains' in standard_metadata: "
            f"expected set, list, or tuple, got {type(domains).__name__}"
        )
