from .base_sql_operator import BaseSqlOperator
from cdisc_rules_engine.enums.static_tables import StaticTables


class ValidMeddraCodeReferenceOperator(BaseSqlOperator):
    """Validates terminology codes against the reference MedDRA dictionary at each level."""

    _code_suffix_map = {"SOCCD": "SOC", "HLGTCD": "HLGT", "HLTCD": "HLT", "PTCD": "PT", "LLTCD": "LLT"}

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target")).lower()

        term_type = None
        for suffix, meddra_type in self._code_suffix_map.items():
            if target_column.upper().endswith(suffix):
                term_type = meddra_type
                break

        if not term_type:
            raise ValueError(f"Could not determine MedDRA term type for code column '{target_column}'")

        query = f"""
            CASE
                WHEN {self._column_sql(target_column, alias=False)} IN (
                    SELECT term_code
                    FROM {StaticTables.MEDDRA_TABLE_NAME.value}
                    WHERE term_type = '{term_type}'
                ) THEN TRUE
                ELSE FALSE
            END
        """
        return self._do_check_operator(lambda: query)
