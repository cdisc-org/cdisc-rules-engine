from .base_sql_operator import BaseSqlOperator


class ValidExDictCodeReferenceOperator(BaseSqlOperator):
    """Validates terminology codes against a reference external dictionary."""

    def __init__(self, data, table_name, suffix_map=None):
        super().__init__(data)
        self.table_name = table_name
        self.suffix_map = suffix_map

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target")).lower()

        type_condition = ""
        if self.suffix_map:
            term_type = None
            for suffix, dict_type in self.suffix_map.items():
                if target_column.upper().endswith(suffix):
                    term_type = dict_type
                    break

            if not term_type:
                raise ValueError(f"Could not determine term type for code column '{target_column}'")
            type_condition = f"WHERE term_type = '{term_type}'"

        query = f"""
            CASE
                WHEN CAST({self._column_sql(target_column, alias=False)} AS TEXT) IN (
                    SELECT term_code
                    FROM {self.table_name}
                    {type_condition}
                ) THEN TRUE
                ELSE FALSE
            END
        """
        return self._do_check_operator(lambda: query)
