from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService
from cdisc_rules_engine.utilities.utils import get_library_variables_metadata_cache_key
from cdisc_rules_engine import config


class VariablePermissibility(BaseOperation):
    def _execute_operation(self):
        """
        Get the variable permissibility values for all data in the current
        dataset.
        """
        cache_key = get_library_variables_metadata_cache_key(
            self.params.standard, self.params.standard_version
        )
        variable_details: dict = self.cache.get(cache_key)
        if variable_details is None:
            cdisc_library_service = CDISCLibraryService(config, self.cache)
            variable_details = cdisc_library_service.get_variables_details(
                self.params.standard, self.params.standard_version
            )
        variable_permissibilities = {
            key: variable_details[key].get("core") for key in variable_details.keys()
        }
        return variable_permissibilities
