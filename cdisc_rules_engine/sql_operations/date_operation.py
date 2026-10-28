from typing import Optional

from cdisc_rules_engine.sql_operations.aggregate_operation import SqlAggregateOperation

# An ISO 8601 date or datetime, where trailing components may be left out and any component may be
# written as "-" when missing, as in the SDTMIG (e.g. 2018-06, 2003---15 or 2018-04-16T-:30).
# The components that are present must be in range, and the year cannot be 0000.
DATE_TIME_PATTERN = (
    r"^(?!0000)(\d{4}|-)(-(0[1-9]|1[0-2]|-)(-(0[1-9]|[12]\d|3[01]|-)"
    r"(T([01]\d|2[0-3]|-)(:([0-5]\d|-)(:([0-5]\d(\.\d+)?|-))?)?(Z|[+-]([01]\d|2[0-3])(:?[0-5]\d)?)?)?)?)?$"
)

# The leading components of such a value, up to the first missing one
KNOWN_COMPONENTS_PATTERN = r"^(\d{4}(-\d{2}(-\d{2}(T\d{2}(:\d{2}(:\d{2}(\.\d+)?)?)?(Z|[+-]\d{2}(:?\d{2})?)?)?)?)?)"


class SqlDateOperation(SqlAggregateOperation):
    """
    The earliest or latest date of a variable, compared as ISO 8601 text so partial dates and
    times take part: a partial date sorts as the start of its period, so 2014-02 comes after
    2014-01-05 but before 2014-02-10. The value is returned as found in the data, except that
    the components after the first missing one are dropped and a space separating date and time
    becomes "T". Time zone offsets are kept but not converted, so values sort by local time.
    """

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
        # The "C" collation compares characters by code, which is what makes ISO 8601 text sort
        # chronologically. Locale collations may weigh punctuation such as "-" differently.
        return f'{self.function}({self._as_iso_date_sql(value_sql)} COLLATE "C"){aggregate_filter}'

    @staticmethod
    def _as_iso_date_sql(value_sql: str) -> str:
        """
        The known leading components of a value as ISO 8601 text, or NULL when the value is not a
        valid ISO 8601 date (garbage, an out of range component like 2025-13-24, an impossible day
        like 2012-12-32, or a missing year), so it is left out of the aggregate. Nothing is cast
        unless the value is known to be valid, and only a nested CASE guarantees that ordering.
        """
        value = f"REPLACE(RTRIM({value_sql}::text), ' ', 'T')"
        known = f"SUBSTRING({value} FROM '{KNOWN_COMPONENTS_PATTERN}')"
        month_start = f"(LEFT({known}, 7) || '-01')::date"
        days_in_month = f"EXTRACT(DAY FROM {month_start} + INTERVAL '1 month' - INTERVAL '1 day')"
        return (
            f"CASE WHEN {value} ~ '{DATE_TIME_PATTERN}' THEN "
            f"CASE WHEN LENGTH({known}) < 10 THEN {known} "
            f"WHEN SUBSTRING({known}, 9, 2)::int <= {days_in_month} THEN {known} END "
            f"END"
        )
