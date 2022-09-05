from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.distinct import Distinct
from cdisc_rules_engine.operations.dy import DY
from cdisc_rules_engine.operations.extract_metadata import ExtractMetadata
from cdisc_rules_engine.operations.max_date import MaxDate
from cdisc_rules_engine.operations.min_date import MinDate
from cdisc_rules_engine.operations.minimum import Minimum
from cdisc_rules_engine.operations.maximum import Maximum
from cdisc_rules_engine.operations.mean import Mean
from cdisc_rules_engine.operations.operation_interface import OperationInterface
from cdisc_rules_engine.operations.valid_meddra_code_references import (
    ValidMeddraCodeReferences,
)
from cdisc_rules_engine.operations.valid_meddra_term_references import (
    ValidMeddraTermReferences,
)
from cdisc_rules_engine.operations.valide_meddra_code_term_pairs import (
    ValidMeddraCodeTermPairs,
)
from cdisc_rules_engine.operations.valide_whodrug_references import (
    ValidWhodrugReferences,
)
from cdisc_rules_engine.operations.variable_exists import VariableExists
from cdisc_rules_engine.operations.variable_names import VariableNames
from cdisc_rules_engine.operations.variable_value_count import VariableValueCount
from cdisc_rules_engine.services.cache.cache_service_interface import (
    CacheServiceInterface,
)
from cdisc_rules_engine.services.data_services.base_data_service import BaseDataService
import pandas as pd
from typing import Type


class OperationsFactory:
    _operations_map = {
        "min": Minimum,
        "max": Maximum,
        "mean": Mean,
        "distinct": Distinct,
        "dy": DY,
        "max_date": MaxDate,
        "min_date": MinDate,
        "extract_metadata": ExtractMetadata,
        "variable_exists": VariableExists,
        "variable_value_count": VariableValueCount,
        "variable_names": VariableNames,
        "valide_meddra_code_references": ValidMeddraCodeReferences,
        "valid_whodrug_references": ValidWhodrugReferences,
        "valid_meddra_term_references": ValidMeddraTermReferences,
        "valid_meddra_code_term_pairs": ValidMeddraCodeTermPairs,
    }

    def __init__(self):
        pass

    @classmethod
    def register_operation(cls, name: str, operation: Type[OperationInterface]) -> None:
        """
        Save mapping of operation name and it's implementation
        """
        if not name:
            raise ValueError("Operation name must not be empty!")
        if not issubclass(operation, OperationInterface):
            raise TypeError("Implementation of OperationInterface required!")
        cls._service_map[name] = operation

    def get_operation(
        self,
        params: OperationParams,
        dataset: pd.DataFrame,
        cache: CacheServiceInterface,
        data_service: BaseDataService,
    ) -> OperationInterface:
        """Get instance of operation that matches operation specified in params"""
        if params.operation_name in self._operations_map:
            return self._operations_map.get(params.operation_name)(
                params, dataset, cache, data_service
            )
        raise ValueError(
            f"Operation name must be in  {list(self._operations_map.keys())}, "
            f"given operation name is {params.operation_name}"
        )
