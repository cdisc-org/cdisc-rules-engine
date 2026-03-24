from .base_sql_operator import BaseSqlOperator
from cdisc_rules_engine.enums.static_tables import StaticTables


class IsValidWhodrugReferenceOperator(BaseSqlOperator):
    """Checks if a value in a target column is a valid WHODrug reference."""

    _attribute_map = {"name": "drug_name", "code": "atc_code", "class": "level_4"}

    def execute_operator(self, other_value):
        target_column = self.replace_prefix(other_value.get("target")).lower()
        attribute = other_value.get("attribute_name")
        if attribute not in self._attribute_map:
            raise ValueError(
                f"Invalid attribute '{attribute}' for IsValidWhodrugReferenceOperator. "
                f"Valid attributes are: {list(self._attribute_map.keys())}."
            )
        whodrug_column = self._attribute_map[attribute]
        query = f"""
            CASE
                WHEN {self._column_sql(target_column, alias=False)} IN (
                    SELECT TRIM(regexp_split_to_table({whodrug_column}, '[,;]'))
                    FROM {StaticTables.WHODRUG_TABLE_NAME.value}
                ) THEN TRUE
                WHEN {self._column_sql(target_column, alias=False)} = 'MULTIPLE' THEN TRUE
                ELSE FALSE
            END
        """
        return self._do_check_operator(lambda: query)
