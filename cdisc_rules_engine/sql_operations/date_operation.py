from typing import Optional

from cdisc_rules_engine.sql_operations.aggregate_operation import SqlAggregateOperation


class SqlDateOperation(SqlAggregateOperation):

    @property
    def operation_name(self) -> str:
        return f"{self.function.lower()}_date"

    def _dataset_value(self, table: str):
        column = self._dataset_column(table)
        return column.hash, column.type

    def _can_aggregate(self, subtype: Optional[str]) -> bool:
        return subtype != "Num"

    def _aggregate_sql(self, value_sql: str, aggregate_filter: str = "") -> str:
        return f"COALESCE({self._group_aggregate_sql(value_sql, aggregate_filter)}, '')"

    def _group_value_sql(self) -> str:
        return "COALESCE({value}, '')"

    def _group_aggregate_sql(self, value_sql: str, aggregate_filter: str) -> str:
        as_date = f"CASE WHEN {value_sql} IS NULL OR {value_sql} = '' THEN NULL ELSE {value_sql}::date END"
        return f"TO_CHAR({self.function}({as_date}){aggregate_filter}, 'YYYY-MM-DD')"
