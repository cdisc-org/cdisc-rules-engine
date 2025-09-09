from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.distinct import SqlDistinct
from cdisc_rules_engine.sql_operations.numeric_operation import (
    SqlNumericOperation,
)
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from cdisc_rules_engine.sql_operations.variable_exists import SqlVariableExists


class SqlOperationsFactory:
    _operations_map = {
        "codelist_extensible": None,
        "codelist_terms": None,
        "dataset_names": None,
        "define_extensible_codelists": None,
        "distinct": SqlDistinct,
        "dy": None,
        "extract_metadata": None,
        "get_column_order_from_dataset": None,
        "get_column_order_from_library": None,
        "get_codelist_attributes": None,
        "get_model_column_order": None,
        "get_model_filtered_variables": None,
        "get_parent_model_column_order": None,
        "map": None,
        "max": lambda params, ds: SqlNumericOperation(params, ds, "MAX"),
        "max_date": None,
        "mean": lambda params, ds: SqlNumericOperation(params, ds, "AVG"),
        "min": lambda params, ds: SqlNumericOperation(params, ds, "MIN"),
        "min_date": None,
        "record_count": lambda params, ds: SqlNumericOperation(params, ds, "COUNT"),
        "valid_meddra_code_references": None,
        "valid_whodrug_references": None,
        "whodrug_code_hierarchy": None,
        "valid_meddra_term_references": None,
        "valid_meddra_code_term_pairs": None,
        "variable_exists": SqlVariableExists,
        "variable_names": None,
        "variable_library_metadata": None,
        "variable_value_count": None,
        "variable_count": None,
        "variable_is_null": None,
        "domain_is_custom": None,
        "domain_label": None,
        "required_variables": None,
        "expected_variables": None,
        "permissible_variables": None,
        "study_domains": None,
        "valid_codelist_dates": None,
        "label_referenced_variable_metadata": None,
        "name_referenced_variable_metadata": None,
        "define_variable_metadata": None,
        "valid_external_dictionary_value": None,
        "valid_external_dictionary_code": None,
        "valid_external_dictionary_code_term_pair": None,
        "valid_define_external_dictionary_version": None,
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
