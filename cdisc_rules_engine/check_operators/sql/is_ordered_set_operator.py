from .base_sql_operator import BaseSqlOperator


class IsOrderedSetOperator(BaseSqlOperator):
    """
    True if the dataset rows are in ascending order of the values within `name`, grouped by the values within `value`.
    """

    def __init__(self, data, invert=False):
        super().__init__(data)
        self.invert = invert

    def _execute_operator_impl(self, other_value):
        name = self.replace_prefix(other_value.get("target"))
        value = other_value.get("comparator")
        value_is_literal = other_value.get("value_is_literal", False)
        if not value_is_literal:
            value = self.replace_prefix(value)
        if self.invert:
            operator_name = f"{name}_is_not_ordered_set_within_{value}"
        else:
            operator_name = f"{name}_is_ordered_set_within_{value}"

        def sql():
            name_hash = self.sql_data_service.pgi.schema.get_column_hash(self.table_id, name)
            value_hash = (
                self.sql_data_service.pgi.schema.get_column_hash(self.table_id, value)
                if not value_is_literal
                else self._constant_sql(value)
            )
            value_column = self._column_sql(value) if not value_is_literal else self._constant_sql(value)

            dataset_is_ordered = f"""
            NOT EXISTS (
                SELECT 1
                FROM {self.table_id} t1
                INNER JOIN {self.table_id} t2 ON t1.{value_hash} = t2.{value_hash}
                WHERE t1.{value_hash} = {value_column}
                AND t1.{name_hash} IS NOT NULL
                AND t2.{name_hash} IS NOT NULL
                AND t1.ctid < t2.ctid
                AND t1.{name_hash} > t2.{name_hash}
            )
            """
            if self.invert:
                return f"""
                CASE
                    WHEN {self._is_empty_sql(name)} THEN FALSE
                    ELSE NOT ({dataset_is_ordered})
                END
                """
            else:
                return f"""
                CASE
                    WHEN {self._is_empty_sql(name)} THEN FALSE
                    ELSE {dataset_is_ordered}
                END
                """

        return self._do_check_operator(operator_name, sql)

    def get_result_for_missing_columns(self):
        return "FALSE"
