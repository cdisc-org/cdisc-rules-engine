from cdisc_rules_engine.operations.base_operation import BaseOperation


class GetLibraryClassDomains(BaseOperation):
    """
    A class for fetching domains for a given class from the CDISC Library.
    Retrieves the list of domains based on standard, version, and optional class filter.
    Example use case: FB1101 - "All Trial Design datasets as listed in the corresponding IG should be submitted"
    """

    def _execute_operation(self):
        domain_class = getattr(self.params, "domain_class", None)
        standard_details = self.library_metadata.standard_metadata
        domains = self._extract_domains(standard_details, domain_class)
        return domains

    def _extract_domains(self, standard_details: dict, domain_class: str = None) -> set:
        domains = set()
        classes = standard_details.get("classes", [])

        for cls in classes:
            if domain_class and cls.get("name") != domain_class:
                continue
            datasets = cls.get("datasets", [])
            for dataset in datasets:
                domain_name = dataset.get("name")
                domains.add(domain_name)

        return domains
