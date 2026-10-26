from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.utilities.sdtm_utilities import is_custom_domain
from cdisc_rules_engine.utilities.utils import get_dataset_standard_check_name


class DomainIsCustom(BaseOperation):
    def _execute_operation(self):
        """
        Gets standard details from cache and checks if
        given domain is in standard domains.
        If no -> the domain is custom.
        """
        check_name = get_dataset_standard_check_name(self.params.dataframe_metadata)
        return is_custom_domain(self.library_metadata, check_name)
