from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.utilities.utils import get_library_variables_metadata_cache_key
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService
from cdisc_rules_engine import config


class VariableNames(BaseOperation):
    def _execute_operation(self):
        """
        Return the set of variable names for the given standard
        """
        cache_key = get_library_variables_metadata_cache_key(
            self.params.standard, self.params.standard_version
        )
        variable_details: dict = self.cache.get(cache_key)
        if variable_details is not None:
            variable_names = set(variable_details.keys())
        else:
            cdisc_library_service = CDISCLibraryService(config, self.cache)
            variable_names = set(
                cdisc_library_service.get_variables_details(
                    self.params.standard, self.params.standard_version
                ).keys()
            )
        return variable_names
