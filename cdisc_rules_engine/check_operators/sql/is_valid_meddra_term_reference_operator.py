from .base_sql_operator import BaseSqlOperator
from cdisc_rules_engine.enums.static_tables import StaticTables


class ValidMeddraTermReferenceOperator(BaseSqlOperator):
    """Validates terms text against the reference MedDRA dictionary at each level."""

    _term_suffix_map = {"SOC": "SOC", "HLGT": "HLGT", "HLT": "HLT", "DECOD": "PT", "LLT": "LLT"}

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target")).lower()

        term_type = None
        for suffix, meddra_type in self._term_suffix_map.items():
            if target_column.upper().endswith(suffix):
                term_type = meddra_type
                break

        if not term_type:
            raise ValueError(f"Could not determine MedDRA term type for term column '{target_column}'")

        query = f"""
            CASE
                WHEN CAST({self._column_sql(target_column, alias=False)} AS TEXT) IN (
                    SELECT term_name
                    FROM {StaticTables.MEDDRA_TABLE_NAME.value}
                    WHERE term_type = '{term_type}'
                ) THEN TRUE
                ELSE FALSE
            END
        """
        return self._do_check_operator(lambda: query)
