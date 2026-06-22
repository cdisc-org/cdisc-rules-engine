from cdisc_rules_engine.enums.static_tables import StaticTables

from .base_sql_operator import BaseSqlOperator


class ValidWHODrugLevelReferenceOperator(BaseSqlOperator):
    """Validates WHODrug level text against a reference external dictionary."""

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target")).lower()

        union_query = " UNION ".join(
            f"SELECT DISTINCT {level} FROM {StaticTables.WHODRUG_TABLE_NAME.value} WHERE {level} IS NOT NULL"
            for level in ["level_1", "level_2", "level_3", "level_4"]
        )

        query = f"""
            CASE
                WHEN CAST({self._column_sql(target_column, alias=False, null_return=True)} AS TEXT) IN (
                    {union_query}
                ) THEN TRUE
                ELSE FALSE
            END
        """
        return self._do_check_operator(lambda: query)
