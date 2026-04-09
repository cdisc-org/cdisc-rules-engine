from .base_sql_operator import BaseSqlOperator


class ValidExDictCodeTermPairsOperator(BaseSqlOperator):
    """Validates corresponding code-term pairs simultaneously against an external dictionary."""

    def __init__(self, data, table_name, pair_map=None):
        super().__init__(data)
        self.table_name = table_name
        self.pair_map = pair_map

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target")).lower()

        term_type = None
        code_col = None
        term_col = None
        type_condition = ""

        if self.pair_map:
            for suffix, (dict_type, counterpart_suffix) in self.pair_map.items():
                if target_column.upper().endswith(suffix):
                    term_type = dict_type
                    base_prefix = target_column[: -len(suffix)]
                    counterpart_column = base_prefix + counterpart_suffix.lower()

                    if suffix.endswith("CD"):
                        code_col = target_column
                        term_col = counterpart_column
                    else:
                        term_col = target_column
                        code_col = counterpart_column
                    break

            if not term_type:
                raise ValueError(f"Could not determine pair types for column '{target_column}'")

            type_condition = f"AND term_type = '{term_type}'"
        else:
            comparator_column = self.replace_prefix(other_value.get("comparator")).lower()
            if target_column.endswith("cd"):
                code_col = target_column
                term_col = comparator_column
            else:
                term_col = target_column
                code_col = comparator_column

        query = f"""
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM {self.table_name}
                    WHERE term_code = CAST({self._column_sql(code_col, alias=False)} AS TEXT)
                      AND term_name = CAST({self._column_sql(term_col, alias=False)} AS TEXT)
                      {type_condition}
                ) THEN TRUE
                ELSE FALSE
            END
        """
        return self._do_check_operator(lambda: query)
