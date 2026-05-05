from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.dataset_column_order import (
    SqlDatasetColumnOrderOperation,
)
from cdisc_rules_engine.sql_operations.dataset_names import SqlDatasetNamesOperation
from cdisc_rules_engine.sql_operations.date_operation import SqlDateOperation
from cdisc_rules_engine.sql_operations.day_data_validator import (
    SqlDayDataValidatorOperation,
)
from cdisc_rules_engine.sql_operations.distinct import SqlDistinctOperation
from cdisc_rules_engine.sql_operations.domain_label import SqlDomainLabelOperation
from cdisc_rules_engine.sql_operations.minus import SqlMinusOperation
from cdisc_rules_engine.sql_operations.numeric_operation import (
    SqlNumericOperation,
)
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from cdisc_rules_engine.sql_operations.variable_exists import SqlVariableExistsOperation
from cdisc_rules_engine.sql_operations.get_model_filtered_variables import SqlGetModelFilteredVariables
from cdisc_rules_engine.sql_operations.variable_count import SqlVariableCountOperation
from cdisc_rules_engine.sql_operations.permissibility_operation import SqlPermissibilityOperation
from cdisc_rules_engine.constants.permissibility import (
    REQUIRED,
    EXPECTED,
    PERMISSIBLE,
)
from cdisc_rules_engine.sql_operations.study_domains import SqlStudyDomainsOperation
from cdisc_rules_engine.sql_operations.get_model_column_order import SqlGetModelColumnOrder
from cdisc_rules_engine.sql_operations.get_parent_model_column_order import SqlGetParentModelColumnOrderOperation
from cdisc_rules_engine.sql_operations.domain_is_custom import SqlDomainIsCustomOperation
from cdisc_rules_engine.sql_operations.valid_codelist_dates import SqlValidCodelistDates
from cdisc_rules_engine.sql_operations.extract_metadata import SqlExtractMetadataOperation
from cdisc_rules_engine.sql_operations.get_codelist_attributes import SqlGetCodelistAttributesOperation
from cdisc_rules_engine.sql_operations.get_define_variables_metadata import (
    SqlGetDefineVariablesMetadata,
)
from cdisc_rules_engine.sql_operations.define_exdict_version_operation import (
    SqlDefineExternalDictionaryVersionOperation,
)
from cdisc_rules_engine.sql_operations.whodrug_code_hierarchy import SqlWhodrugHierarchyOperation
from cdisc_rules_engine.sql_operations.standard_domains import SqlStandardDomainsOperation
from cdisc_rules_engine.sql_operations.label_referenced_variable_metadata import SqlLabelReferencedVariableMetadata
from cdisc_rules_engine.sql_operations.name_referenced_variable_metadata import SqlNameReferencedVariableMetadata


class SqlOperationsFactory:
    _operations_map = {
        "codelist_extensible": None,
        "codelist_terms": None,
        "dataset_names": SqlDatasetNamesOperation,
        "define_extensible_codelists": None,
        "distinct": SqlDistinctOperation,
        "dy": SqlDayDataValidatorOperation,
        "extract_metadata": SqlExtractMetadataOperation,
        "get_column_order_from_dataset": SqlDatasetColumnOrderOperation,
        "get_column_order_from_library": None,
        "get_codelist_attributes": SqlGetCodelistAttributesOperation,
        "get_model_column_order": SqlGetModelColumnOrder,
        "get_model_filtered_variables": SqlGetModelFilteredVariables,
        "get_parent_model_column_order": SqlGetParentModelColumnOrderOperation,
        "map": None,
        "max": lambda params, ds: SqlNumericOperation(params, ds, "MAX"),
        "max_date": lambda params, ds: SqlDateOperation(params, ds, "MAX"),
        "mean": lambda params, ds: SqlNumericOperation(params, ds, "AVG"),
        "min": lambda params, ds: SqlNumericOperation(params, ds, "MIN"),
        "min_date": lambda params, ds: SqlDateOperation(params, ds, "MIN"),
        "minus": SqlMinusOperation,
        "record_count": lambda params, ds: SqlNumericOperation(params, ds, "COUNT"),
        "whodrug_code_hierarchy": SqlWhodrugHierarchyOperation,
        "variable_exists": SqlVariableExistsOperation,
        "variable_names": None,
        "variable_library_metadata": None,
        "variable_value_count": None,
        "variable_count": SqlVariableCountOperation,
        "variable_is_null": None,
        "domain_is_custom": SqlDomainIsCustomOperation,
        "domain_label": SqlDomainLabelOperation,
        "required_variables": lambda params, ds: SqlPermissibilityOperation(params, ds, REQUIRED),
        "expected_variables": lambda params, ds: SqlPermissibilityOperation(params, ds, EXPECTED),
        "permissible_variables": lambda params, ds: SqlPermissibilityOperation(params, ds, PERMISSIBLE),
        "standard_domains": SqlStandardDomainsOperation,
        "study_domains": SqlStudyDomainsOperation,
        "valid_codelist_dates": SqlValidCodelistDates,
        "label_referenced_variable_metadata": SqlLabelReferencedVariableMetadata,
        "name_referenced_variable_metadata": SqlNameReferencedVariableMetadata,
        "define_variable_metadata": SqlGetDefineVariablesMetadata,
        "valid_external_dictionary_value": None,
        "valid_external_dictionary_code": None,
        "valid_external_dictionary_code_term_pair": None,
        "valid_define_external_dictionary_version": None,
        "get_define_external_dictionary_version": SqlDefineExternalDictionaryVersionOperation,
        "get_dataset_filtered_variables": None,
    }

    @classmethod
    def get_service(
        cls,
        name: str,
        params: SqlOperationParams,
        data_service: PostgresQLDataService,
    ) -> SqlBaseOperation:
        if name in cls._operations_map:
            operation = cls._operations_map.get(name)
            if operation is None:
                raise NotImplementedError(f"Operation {name} is not implemented")

            return operation(params, data_service)

        raise ValueError(
            f"Operation name must be in  {list(cls._operations_map.keys())}, " f"given operation name is {name}"
        )
