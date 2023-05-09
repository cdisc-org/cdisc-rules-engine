from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.utilities.utils import (
    search_in_list_of_dicts,
)


class DomainLabel(BaseOperation):
    def _execute_operation(self):
        """
        Return the domain label for the currently executing domain
        """
        standard_data = self._retrieve_standards_metadata()
        domain_details = None
        for c in standard_data.get("classes", []):
            domain_details = search_in_list_of_dicts(
                c.get("datasets", []), lambda item: item["name"] == self.params.domain
            )
            if domain_details:
                return domain_details.get("label", "")
        return ""
