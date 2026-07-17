from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation
from typing import Literal


class SqlMinusOperation(SqlBaseOperation):
    def _execute_operation(self):
        """
        Executes a set difference (MINUS/EXCEPT) operation.
        Returns the values present in 'name' that are NOT present in 'subtract'.
        """
        domain = self.params.domain
        dataset_id = self.data_service.pgi.schema.get_table_hash(domain)
        case_sensitive = self.params.case_sensitive

        name_param = self.params.name
        subtract_param = self.params.subtract

        name_query = self._remap_params(
            self._resolve_param(
                name_param,
                domain,
                dataset_id,
                "query",
            ),
            "name",
        )
        subtract_query = self._remap_params(
            self._resolve_param(
                subtract_param,
                domain,
                dataset_id,
                "query",
            ),
            "subtract",
        )

        case_value = "UPPER(value)" if not case_sensitive else "value"

        case_value = "UPPER(value)" if not case_sensitive else "value"

        query = f"""
            SELECT {case_value} AS value FROM ({name_query}) AS name_q
            EXCEPT
            SELECT {case_value} AS value FROM ({subtract_query}) AS subtract_q
        """

        name_params = self._remap_params(
            self._resolve_param(
                name_param,
                domain,
                dataset_id,
                "param",
            ),
            "name",
        )
        subtract_params = self._remap_params(
            self._resolve_param(
                subtract_param,
                domain,
                dataset_id,
                "param",
            ),
            "subtract",
        )

        params = {**name_params, **subtract_params}

        return SqlOperationResult(query=query, type="collection", subtype="Char", params=params or None)

    def _resolve_param(self, param_val: str, domain: str, dataset_id: str, q_or_p: Literal["query", "param"]) -> str:
        if self._column_exists_in_domain(domain, param_val):
            col_hash = self.data_service.pgi.schema.get_column_hash(domain, param_val)
            return f"SELECT {col_hash} AS value FROM {dataset_id} WHERE {col_hash} IS NOT NULL"
        elif isinstance(param_val, list):
            return self._format_variable_list_to_query(vars=param_val, unique=True)
        elif self._get_previous_operation(param_val):
            return (
                self._get_previous_operation(param_val).query
                if q_or_p == "query"
                else self._get_previous_operation(param_val).params
            )

        return {} if q_or_p == "param" else self._format_variable_list_to_query(vars=[], unique=True)

    def _remap_params(self, element: str | dict, param_name: str) -> str:
        if not element:
            return {}
        if isinstance(element, dict):
            return {self._remap_params(k, param_name): v for k, v in element.items()}
        else:
            return element.replace("$", f"${param_name}_")
