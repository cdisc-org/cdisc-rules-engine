from typing import Optional

from cdisc_rules_engine.sql_operations.aggregate_operation import SqlAggregateOperation

# A complete ISO 8601 date, optionally followed by a time part, which is not inspected
FULL_DATE_PATTERN = r"^(?!0000)\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])([T ].*)?$"


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
        return f"TO_CHAR({self.function}({self._as_date_sql(value_sql)}){aggregate_filter}, 'YYYY-MM-DD')"

    @staticmethod
    def _as_date_sql(value_sql: str) -> str:
        """
        The date part of a value, or NULL when the value is not a complete, real calendar date
        (thus excluding partial dates like 2018-06 or impossible ones like 2012-12-32).
        The day is checked against the length of its month in a nested CASE.
        """
        text = f"{value_sql}::text"
        month_start = f"(LEFT({text}, 7) || '-01')::date"
        days_in_month = f"EXTRACT(DAY FROM {month_start} + INTERVAL '1 month' - INTERVAL '1 day')"
        return (
            f"CASE WHEN {text} ~ '{FULL_DATE_PATTERN}' THEN "
            f"CASE WHEN SUBSTRING({text}, 9, 2)::int <= {days_in_month} THEN LEFT({text}, 10)::date END "
            f"END"
        )
