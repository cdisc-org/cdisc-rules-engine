from typing import List, Optional, Tuple

from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.services import logger
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
    DEFINE_VLM_TYPE,
)

SQL_TYPES = {"Bool": "BOOLEAN", "Num": "NUMERIC", "Char": "TEXT"}
NUMERIC_COMPARATORS = {"LT": "<", "LE": "<=", "GT": ">", "GE": ">=", "EQ": "=", "NE": "<>"}
ROW_NUMBER_ALIAS = "vlm_source_row_number"
SOURCE_CTE = "vlm_source"


class UntranslatableWhereClause(Exception):
    """Raised when a Define where clause cannot be expressed as SQL against the source dataset."""


class SqlValueCheckAgainstDefineVLMDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Returns a long dataset (unpivoted) where each value in each row of the original dataset
    that is described by Define XML value level metadata is a row in the new dataset.
    A value is described by a VLM when the record matches the VLM's where clause.
    Only variable values that have VLM attached are produced.

    Columns available in the dataset are:
        row_number, variable_name, variable_value, define_variable_name, define_vlm_...
    plus all columns of the source record, so a rule can check other variables of the record
    that the VLM applies to.
    """

    def build(self) -> str:
        table_name = f"{self.dataset_metadata.name}_value_check_define_vlm"
        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        source_table_id = self.data_service.get_dataset_for_rule(
            self.dataset_metadata, self.rule, self.standards_context
        )
        source_schema = self.data_service.pgi.schema.get_table(source_table_id)
        source_table_hash = self.data_service.pgi.schema.get_table_hash(source_table_id)

        vlm_columns = {
            "row_number": "Num",
            "variable_name": "Char",
            "variable_value": "Char",
            "define_variable_name": "Char",
            **DEFINE_VLM_TYPE,
        }
        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)
        for col, type in vlm_columns.items():
            schema.add_column(SqlColumnSchema.generated(col, type))

        source_columns = self._get_source_columns(source_schema, vlm_columns)
        for name, sc in source_columns:
            schema.add_column(SqlColumnSchema.generated(name, sc.type))

        self.data_service.pgi.create_table(schema)

        select_statements = [
            statement
            for statement in (self._vlm_select(vlm, source_schema, source_columns) for vlm in self.get_define_vlms())
            if statement
        ]

        if select_statements:
            target_columns = [schema.get_column_hash(col) for col in vlm_columns] + [
                schema.get_column_hash(name) for name, _ in source_columns
            ]
            insert_query = (
                f"INSERT INTO {schema.hash} ({', '.join(target_columns)}) "
                f"WITH {SOURCE_CTE} AS (SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS {ROW_NUMBER_ALIAS} "
                f"FROM {source_table_hash}) "
                f"{' UNION ALL '.join(select_statements)};"
            )
            self.data_service.pgi.execute_sql(insert_query)

        return table_name

    @staticmethod
    def _get_source_columns(source_schema: SqlTableSchema, vlm_columns: dict) -> List[Tuple[str, SqlColumnSchema]]:
        source_columns = []
        for name, sc in source_schema.get_columns():
            if name.lower() == "id" or sc.origin != "data":
                continue
            if name.lower() in vlm_columns:
                logger.warning(f"Source column '{name}' clashes with a VLM dataset column and is not available")
                continue
            source_columns.append((name, sc))
        return source_columns

    @classmethod
    def _vlm_select(cls, vlm: dict, source_schema: SqlTableSchema, source_columns: list) -> Optional[str]:
        variable_name = (vlm.get("define_variable_name") or "").upper()
        value_hash = source_schema.get_column_hash(variable_name)
        if not value_hash:
            logger.warning(f"Skipping VLM for '{variable_name}': variable is not in the dataset")
            return None
        try:
            where = cls._where_clause_to_sql(vlm.get("where_clause"), source_schema)
        except UntranslatableWhereClause as e:
            logger.warning(f"Skipping VLM for '{variable_name}': {e}")
            return None

        safe_name = variable_name.replace("'", "''")
        select_parts = [
            ROW_NUMBER_ALIAS,
            f"CAST('{safe_name}' AS TEXT)",
            f"CAST({SOURCE_CTE}.{value_hash} AS TEXT)",
            f"CAST('{safe_name}' AS TEXT)",
        ]
        select_parts += [cls._literal(vlm.get(key), type) for key, type in DEFINE_VLM_TYPE.items()]
        select_parts += [f"{SOURCE_CTE}.{sc.hash}" for _, sc in source_columns]
        return f"SELECT {', '.join(select_parts)} FROM {SOURCE_CTE} WHERE {where}"

    @staticmethod
    def _literal(value, type: str) -> str:
        sql_type = SQL_TYPES.get(type, "TEXT")
        if value is None or str(value).strip() == "":
            return f"CAST(NULL AS {sql_type})"
        escaped = str(value).strip().replace("'", "''")
        return f"CAST('{escaped}' AS {sql_type})"

    @classmethod
    def _where_clause_to_sql(cls, where_clause: List[dict], source_schema: SqlTableSchema) -> str:
        """ANDs every range check of the where clause."""
        if not where_clause:
            raise UntranslatableWhereClause("where clause is missing or references an item not in the define")
        conditions = []
        for check in where_clause:
            column = source_schema.get_column(check["variable"])
            if column is None:
                raise UntranslatableWhereClause(f"where clause variable '{check['variable']}' is not in the dataset")
            condition = cls._range_check_to_sql(
                f"{SOURCE_CTE}.{column.hash}", column.type, check["comparator"], check["values"]
            )
            conditions.append(f"({condition})")
        return " AND ".join(conditions)

    @classmethod
    def _range_check_to_sql(cls, column: str, column_type: str, comparator: str, values: List[str]) -> str:
        """
        Missing values behave the same for numeric and text columns: they never satisfy an
        ordering comparator, they equal an empty check value, and they satisfy NE / NOTIN.
        """
        if not values:
            raise UntranslatableWhereClause(f"range check with comparator '{comparator}' has no check values")
        if comparator in ("LT", "LE", "GT", "GE"):
            return cls._ordering_to_sql(column, column_type, comparator, values[0])
        if comparator in ("EQ", "NE"):
            values = values[:1]
        elif comparator not in ("IN", "NOTIN"):
            raise UntranslatableWhereClause(f"unsupported comparator '{comparator}'")

        matches = f"COALESCE({cls._membership_to_sql(column, column_type, values)}, FALSE)"
        return matches if comparator in ("EQ", "IN") else f"NOT {matches}"

    @staticmethod
    def _ordering_to_sql(column: str, column_type: str, comparator: str, value: str) -> str:
        if column_type == "Num":
            try:
                return f"{column} {NUMERIC_COMPARATORS[comparator]} {float(value)!r}"
            except ValueError:
                raise UntranslatableWhereClause(f"'{value}' is not numeric but is compared to a numeric column")
        literal = "'" + str(value).replace("'", "''") + "'"
        return f"COALESCE(NULLIF(CAST({column} AS TEXT), '') {NUMERIC_COMPARATORS[comparator]} {literal}, FALSE)"

    @staticmethod
    def _membership_to_sql(column: str, column_type: str, values: List[str]) -> str:
        """SQL that is true when the column equals one of the values; NULL is only matched by ''."""
        if column_type != "Num":
            literals = ["'" + str(value).replace("'", "''") + "'" for value in values]
            return f"COALESCE(CAST({column} AS TEXT), '') IN ({', '.join(literals)})"

        numbers = []
        matches_missing = False
        for value in values:
            if str(value).strip() == "":
                matches_missing = True
                continue
            try:
                numbers.append(repr(float(value)))
            except ValueError:
                pass
        parts = []
        if numbers:
            parts.append(f"{column} IN ({', '.join(numbers)})")
        if matches_missing:
            parts.append(f"{column} IS NULL")
        return " OR ".join(parts) if parts else "FALSE"
