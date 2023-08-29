from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService
from cdisc_rules_engine import config


class VariableLibraryMetadata(BaseOperation):
    def _execute_operation(self):
        """
        Get the variable permissibility values for all data in the current
        dataset.
        """
        variable_details: dict = self.library_metadata.variables_metadata
        if not variable_details:
            cdisc_library_service = CDISCLibraryService(config, self.cache)
            variable_details = cdisc_library_service.get_variables_details(
                self.params.standard, self.params.standard_version
            )
        dataset_variable_details = variable_details.get(self.params.domain, {})
        variable_metadata = {
            key: dataset_variable_details[key].get(self.params.target)
            for key in dataset_variable_details.keys()
        }
        return variable_metadata
