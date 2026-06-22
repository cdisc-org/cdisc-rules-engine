from cdisc_rules_engine.enums.static_tables import StaticTables

from .base_sql_operator import BaseSqlOperator


class ValidExDictTermReferenceOperator(BaseSqlOperator):
    """Validates terms text against a reference external dictionary."""

    def __init__(self, data, table_name):
        super().__init__(data)
        self.table_name = table_name

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target")).lower()

        filter_attribute, filter_value = self._filter_params(other_value, self.table_name)

        filter_conditions = []
        whodrug_condition = ""
        case_insensitive = other_value.get("case_insensitive", False)

        if filter_attribute and filter_value:
            filter_conditions.append(f"{filter_attribute} = '{filter_value}'")

        if self.table_name == StaticTables.WHODRUG_TABLE_NAME.value:
            if filter_attribute == "class":
                filter_conditions.append(f"('{filter_value}' IN (level_1, level_2, level_3, level_4))")

            whodrug_condition = (
                f"WHEN {self._column_sql(target_column, alias=False, null_return=True)} = 'MULTIPLE' THEN TRUE"
            )

        cast_expr = f"CAST({self._column_sql(target_column, alias=False, null_return=True)} AS TEXT)"
        term_expr = "TRIM(regexp_split_to_table(term_name, '[,;]'))"
        if case_insensitive:
            cast_expr = f"LOWER({cast_expr})"
            term_expr = f"LOWER({term_expr})"

        query = f"""
            CASE
                WHEN {cast_expr} IN (
                    SELECT {term_expr}
                    FROM {self.table_name}
                    {'WHERE ' + ' AND '.join(filter_conditions) if filter_conditions else ''}
                ) THEN TRUE
                {whodrug_condition}
                ELSE FALSE
            END
        """
        return self._do_check_operator(lambda: query)
