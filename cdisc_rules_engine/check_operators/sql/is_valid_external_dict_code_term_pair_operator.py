from cdisc_rules_engine.enums.static_tables import StaticTables

from .base_sql_operator import BaseSqlOperator


class ValidExDictCodeTermPairsOperator(BaseSqlOperator):
    """Validates corresponding code-term pairs simultaneously against an external dictionary."""

    def __init__(self, data, table_name):
        super().__init__(data)
        self.table_name = table_name

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target")).lower()
        comparator_column = self.replace_prefix(other_value.get("comparator")).lower()

        if comparator_column.endswith("cd"):
            code_column = comparator_column
            term_column = target_column
        else:
            code_column = target_column
            term_column = comparator_column

        filter_attribute, filter_value = self._filter_params(other_value, self.table_name)

        filter_conditions = []
        whodrug_condition = ""
        case_insensitive = other_value.get("case_insensitive", False)

        if filter_attribute and filter_value:
            filter_conditions.append(f"{filter_attribute} = '{filter_value}'")

        if self.table_name == StaticTables.WHODRUG_TABLE_NAME.value:
            if filter_attribute == "class":
                filter_conditions.append(f"('{filter_value}' IN (level_1, level_2, level_3, level_4))")

            whodrug_condition = f"WHEN {self._column_sql(target_column, alias=False)} = 'MULTIPLE' THEN TRUE"

        if case_insensitive:
            comp_expr = f"""
            LOWER(term_code) = LOWER(CAST({self._column_sql(code_column, alias=False)} AS TEXT))
            AND LOWER(term_name) = LOWER(CAST({self._column_sql(term_column, alias=False)} AS TEXT))
            """
        else:
            comp_expr = f"""
            term_code = CAST({self._column_sql(code_column, alias=False)} AS TEXT)
            AND term_name = CAST({self._column_sql(term_column, alias=False)} AS TEXT)
            """

        query = f"""
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM {self.table_name}
                    WHERE {comp_expr}
                        {'AND ' + ' AND '.join(filter_conditions) if filter_conditions else ''}
                ) THEN TRUE
                {whodrug_condition}
                ELSE FALSE
            END
        """
        return self._do_check_operator(lambda: query)
