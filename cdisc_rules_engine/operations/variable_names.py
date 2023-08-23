from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService
from cdisc_rules_engine import config


class VariableNames(BaseOperation):
    def _execute_operation(self):
        """
        Return the set of variable names for the given standard
        """
        variable_details = self.library_metadata.variables_metadata
        if not variable_details:
            cdisc_library_service = CDISCLibraryService(config, self.cache)
            variable_details = cdisc_library_service.get_variables_details(
                self.params.standard, self.params.standard_version
            )
        all_variables = [
            list(variable_details[dataset].values()) for dataset in variable_details
        ]

        return set(
            [variable["name"] for variables in all_variables for variable in variables]
        )
