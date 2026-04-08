from .base_sql_operator import BaseSqlOperator
from cdisc_rules_engine.enums.static_tables import StaticTables


class ValidMeddraCodeTermPairsOperator(BaseSqlOperator):
    """Validates corresponding MedDRA code-term pairs simultaneously."""

    _pair_map = {
        "SOC": ("SOC", "SOCCD"),
        "SOCCD": ("SOC", "SOC"),
        "LLT": ("LLT", "LLTCD"),
        "LLTCD": ("LLT", "LLT"),
        "HLGT": ("HLGT", "HLGTCD"),
        "HLGTCD": ("HLGT", "HLGT"),
        "HLT": ("HLT", "HLTCD"),
        "HLTCD": ("HLT", "HLT"),
        "PTCD": ("PT", "DECOD"),
        "DECOD": ("PT", "PTCD"),
    }

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target")).lower()

        term_type = None
        code_col = None
        term_col = None

        for suffix, (meddra_type, counterpart_suffix) in self._pair_map.items():
            if target_column.upper().endswith(suffix):
                term_type = meddra_type

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
            raise ValueError(f"Could not determine MedDRA pair types for column '{target_column}'")

        query = f"""
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM {StaticTables.MEDDRA_TABLE_NAME.value}
                    WHERE term_code = CAST({self._column_sql(code_col, alias=False)} AS TEXT)
                      AND term_name = CAST({self._column_sql(term_col, alias=False)} AS TEXT)
                      AND term_type = '{term_type}'
                ) THEN TRUE
                ELSE FALSE
            END
        """
        return self._do_check_operator(lambda: query)
