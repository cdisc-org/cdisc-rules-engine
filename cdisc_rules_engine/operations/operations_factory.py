from typing import Type

from cdisc_rules_engine.interfaces import FactoryInterface
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.operations.dataset_column_order import DatasetColumnOrder
from cdisc_rules_engine.operations.day_data_validator import DayDataValidator
from cdisc_rules_engine.operations.distinct import Distinct
from cdisc_rules_engine.operations.extract_metadata import ExtractMetadata
from cdisc_rules_engine.operations.library_column_order import LibraryColumnOrder
from cdisc_rules_engine.operations.max_date import MaxDate
from cdisc_rules_engine.operations.maximum import Maximum
from cdisc_rules_engine.operations.mean import Mean
from cdisc_rules_engine.operations.meddra_code_references_validator import (
    MedDRACodeReferencesValidator,
)
from cdisc_rules_engine.operations.meddra_code_term_pairs_validator import (
    MedDRACodeTermPairsValidator,
)
from cdisc_rules_engine.operations.meddra_term_references_validator import (
    MedDRATermReferencesValidator,
)
from cdisc_rules_engine.operations.min_date import MinDate
from cdisc_rules_engine.operations.minimum import Minimum
from cdisc_rules_engine.operations.variable_exists import VariableExists
from cdisc_rules_engine.operations.variable_names import VariableNames
from cdisc_rules_engine.operations.variable_value_count import VariableValueCount
from cdisc_rules_engine.operations.whodrug_references_validator import (
    WhodrugReferencesValidator,
)


class OperationsFactory(FactoryInterface):
    _operations_map = {
        "min": Minimum,
        "max": Maximum,
        "mean": Mean,
        "distinct": Distinct,
        "dy": DayDataValidator,
        "max_date": MaxDate,
        "min_date": MinDate,
        "extract_metadata": ExtractMetadata,
        "get_column_order_from_dataset": DatasetColumnOrder,
        "get_column_order_from_library": LibraryColumnOrder,
        "variable_exists": VariableExists,
        "variable_value_count": VariableValueCount,
        "variable_names": VariableNames,
        "valide_meddra_code_references": MedDRACodeReferencesValidator,
        "valid_whodrug_references": WhodrugReferencesValidator,
        "valid_meddra_term_references": MedDRATermReferencesValidator,
        "valid_meddra_code_term_pairs": MedDRACodeTermPairsValidator,
    }

    @classmethod
    def register_service(cls, name: str, operation: Type[BaseOperation]) -> None:
        """
        Save mapping of operation name and it's implementation
        """
        if not name:
            raise ValueError("Operation name must not be empty!")
        if not issubclass(operation, BaseOperation):
            raise TypeError("Implementation of BaseOperation required!")
        cls._operations_map[name] = operation

    def get_service(
        self,
        name,
        **kwargs,
    ) -> BaseOperation:
        """Get instance of operation that matches operation specified in params"""
        required_args = {
            "operation_params",
            "original_dataset",
            "cache",
            "data_service",
        }
        if not required_args.issubset(kwargs.keys()):
            raise ValueError(
                f"One of the following required key word arguments is missing: "
                f"{required_args}"
            )
        if name in self._operations_map:
            return self._operations_map.get(name)(
                kwargs.get("operation_params"),
                kwargs.get("original_dataset"),
                kwargs.get("cache"),
                kwargs.get("data_service"),
            )
        raise ValueError(
            f"Operation name must be in  {list(self._operations_map.keys())}, "
            f"given operation name is {name}"
        )
