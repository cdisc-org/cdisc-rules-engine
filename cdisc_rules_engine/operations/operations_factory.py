from typing import Type

from cdisc_rules_engine.interfaces import FactoryInterface
from cdisc_rules_engine.operations.base_operation import BaseOperation
from cdisc_rules_engine.operations.dataset_names import DatasetNames
from cdisc_rules_engine.operations.dataset_column_order import DatasetColumnOrder
from cdisc_rules_engine.operations.day_data_validator import DayDataValidator
from cdisc_rules_engine.operations.define_dictionary_version_validator import (
    DefineDictionaryVersionValidator,
)
from cdisc_rules_engine.operations.distinct import Distinct
from cdisc_rules_engine.operations.extract_metadata import ExtractMetadata
from cdisc_rules_engine.operations.library_column_order import LibraryColumnOrder
from cdisc_rules_engine.operations.library_model_column_order import (
    LibraryModelColumnOrder,
)
from cdisc_rules_engine.operations.map import Map
from cdisc_rules_engine.operations.parent_library_model_column_order import (
    ParentLibraryModelColumnOrder,
)
from cdisc_rules_engine.operations.get_codelist_attributes import (
    CodeListAttributes,
)
from cdisc_rules_engine.operations.get_model_filtered_variables import (
    LibraryModelVariablesFilter,
)
from cdisc_rules_engine.operations.max_date import MaxDate
from cdisc_rules_engine.operations.maximum import Maximum
from cdisc_rules_engine.operations.mean import Mean
from cdisc_rules_engine.operations.domain_is_custom import DomainIsCustom
from cdisc_rules_engine.operations.domain_label import DomainLabel
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
from cdisc_rules_engine.operations.record_count import RecordCount
from cdisc_rules_engine.operations.valid_external_dictionary_code import (
    ValidExternalDictionaryCode,
)
from cdisc_rules_engine.operations.valid_external_dictionary_code_term_pair import (
    ValidExternalDictionaryCodeTermPair,
)
from cdisc_rules_engine.operations.variable_exists import VariableExists
from cdisc_rules_engine.operations.variable_names import VariableNames
from cdisc_rules_engine.operations.variable_value_count import VariableValueCount
from cdisc_rules_engine.operations.whodrug_references_validator import (
    WhodrugReferencesValidator,
)
from cdisc_rules_engine.operations.whodrug_hierarchy_validator import (
    WhodrugHierarchyValidator,
)
from cdisc_rules_engine.operations.variable_count import VariableCount
from cdisc_rules_engine.operations.variable_is_null import VariableIsNull
from cdisc_rules_engine.operations.required_variables import RequiredVariables
from cdisc_rules_engine.operations.expected_variables import ExpectedVariables
from cdisc_rules_engine.operations.permissible_variables import PermissibleVariables
from cdisc_rules_engine.operations.study_domains import StudyDomains
from cdisc_rules_engine.operations.valid_codelist_dates import ValidCodelistDates
from cdisc_rules_engine.operations.label_referenced_variable_metadata import (
    LabelReferencedVariableMetadata,
)
from cdisc_rules_engine.operations.name_referenced_variable_metadata import (
    NameReferencedVariableMetadata,
)
from cdisc_rules_engine.operations.define_variable_metadata import (
    DefineVariableMetadata,
)
from cdisc_rules_engine.operations.valid_external_dictionary_value import (
    ValidExternalDictionaryValue,
)
from cdisc_rules_engine.operations.codelist_terms import CodelistTerms
from cdisc_rules_engine.operations.codelist_extensible import CodelistExtensible
from cdisc_rules_engine.operations.define_xml_extensible_codelists import (
    DefineCodelists,
)
from cdisc_rules_engine.operations.get_dataset_filtered_variables import (
    GetDatasetFilteredVariables,
)


class OperationsFactory(FactoryInterface):
    _operations_map = {
        "codelist_extensible": CodelistExtensible,
        "codelist_terms": CodelistTerms,
        "dataset_names": DatasetNames,
        "define_extensible_codelists": DefineCodelists,
        "distinct": Distinct,
        "dy": DayDataValidator,
        "extract_metadata": ExtractMetadata,
        "get_column_order_from_dataset": DatasetColumnOrder,
        "get_column_order_from_library": LibraryColumnOrder,
        "get_codelist_attributes": CodeListAttributes,
        "get_model_column_order": LibraryModelColumnOrder,
        "get_model_filtered_variables": LibraryModelVariablesFilter,
        "get_parent_model_column_order": ParentLibraryModelColumnOrder,
        "map": Map,
        "max": Maximum,
        "max_date": MaxDate,
        "mean": Mean,
        "min": Minimum,
        "min_date": MinDate,
        "record_count": RecordCount,
        "valid_meddra_code_references": MedDRACodeReferencesValidator,
        "valid_whodrug_references": WhodrugReferencesValidator,
        "whodrug_code_hierarchy": WhodrugHierarchyValidator,
        "valid_meddra_term_references": MedDRATermReferencesValidator,
        "valid_meddra_code_term_pairs": MedDRACodeTermPairsValidator,
        "variable_exists": VariableExists,
        "variable_names": VariableNames,
        "variable_value_count": VariableValueCount,
        "variable_count": VariableCount,
        "variable_is_null": VariableIsNull,
        "domain_is_custom": DomainIsCustom,
        "domain_label": DomainLabel,
        "required_variables": RequiredVariables,
        "expected_variables": ExpectedVariables,
        "permissible_variables": PermissibleVariables,
        "study_domains": StudyDomains,
        "valid_codelist_dates": ValidCodelistDates,
        "label_referenced_variable_metadata": LabelReferencedVariableMetadata,
        "name_referenced_variable_metadata": NameReferencedVariableMetadata,
        "define_variable_metadata": DefineVariableMetadata,
        "valid_external_dictionary_value": ValidExternalDictionaryValue,
        "valid_external_dictionary_code": ValidExternalDictionaryCode,
        "valid_external_dictionary_code_term_pair": ValidExternalDictionaryCodeTermPair,
        "valid_define_external_dictionary_version": DefineDictionaryVersionValidator,
        "get_dataset_filtered_variables": GetDatasetFilteredVariables,
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
            "library_metadata",
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
                kwargs.get("library_metadata"),
            )
        raise ValueError(
            f"Operation name must be in  {list(self._operations_map.keys())}, "
            f"given operation name is {name}"
        )
