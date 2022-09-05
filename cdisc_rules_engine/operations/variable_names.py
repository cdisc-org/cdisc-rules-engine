import pandas as pd
from cdisc_rules_engine.operations.operation_interface import OperationInterface
from cdisc_rules_engine.utilities.utils import get_library_variables_metadata_cache_key
from cdisc_rules_engine.services.cdisc_library_service import CDISCLibraryService
from cdisc_rules_engine import config


class VariableNames(OperationInterface):
    def execute(self) -> pd.DataFrame:
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
        return self._handle_operation_result(
            [variable_names] * len(self.evaluation_dataset)
        )
