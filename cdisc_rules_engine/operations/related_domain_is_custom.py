from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.utilities.sdtm_utilities import is_custom_domain


class RelatedDomainIsCustom(BaseOperation):
    def _execute_operation(self):
        """
        Gets standard details from cache and checks if
        given domain is in standard domains.
        If no -> the domain is custom.
        """

        for ds in self.params.datasets:
            if ds.is_supp and self.params.domain.endswith(ds.rdomain):
                return is_custom_domain(self.library_metadata, ds.rdomain)
        return False
