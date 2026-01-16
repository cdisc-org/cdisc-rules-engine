from typing import List

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema


class SqlSuppMerge:
    @staticmethod
    def perform_join(
        pgi: PostgresQLInterface,
        original: SqlTableSchema,
        supp: SqlTableSchema,
        domain: str,
    ) -> SqlTableSchema:
        """
        Adds the additional variables defined by the SUPP merge to the input dataset.

        The output table will have all of the columns from the left table as before.
        Any columns defined in QNAM will be added as columns to the output table.
        Any cells which do not have a corresponding record in SUPP will be set to NULL.
        """

        required_supp_columns = SqlSuppMerge._get_required_supp_columns(domain)
        required_original_columns = SqlSuppMerge._get_required_original_columns(pgi, supp, domain)
        new_columns = SqlSuppMerge._determine_new_columns(pgi, supp)

        SqlSuppMerge._validate_merge(original, supp, required_original_columns, required_supp_columns, new_columns)

        name = f"{original.name}_SUPP"
        schema = SqlTableSchema.derived(name, pgi)

        # Check if the table already exists
        if pgi.schema.get_table(name) is not None:
            return pgi.schema.get_table(name)

        # Build the new schema
        schema = SqlSuppMerge._build_schemas(name=name, original=original, new_columns=new_columns, pgi=pgi)

        pgi.create_table(schema)

        queries = []

        # Copy everything from original table
        new_selectors = ", ".join(schema.get_column_hash(col) for col, _ in original.get_columns() if col != "id")
        old_selectors = ", ".join(schema.hash for _, schema in original.get_columns() if schema.name != "id")
        queries.append(
            f"""
            INSERT INTO {schema.hash} ({new_selectors})
                SELECT
                    {old_selectors}
                FROM {original.hash}"""
        )

        # Splitting each new column into its own query to avoid a the arbitrarily large number of
        # joins which would be required to do everything in one go
        for linking_clauses in SqlSuppMerge._get_linking_clauses(original, supp, required_original_columns):
            for col in new_columns:
                queries.append(
                    f"""
                    UPDATE {schema.hash} AS original
                        SET {schema.get_column_hash(col)} = supp.{supp.get_column_hash("QVAL")}
                        FROM {supp.hash} AS supp
                        WHERE {linking_clauses}
                            AND supp.{supp.get_column_hash("QNAM")} = '{col}'"""
                )

        pgi.execute_many(queries)

        return schema

    @staticmethod
    def _build_schemas(
        name: str,
        original: SqlTableSchema,
        new_columns: List[str],
        pgi: PostgresQLInterface,
    ) -> SqlTableSchema:
        """Build the output schema."""

        joined_schema = SqlTableSchema.derived(name, pgi)

        # Add all of the original table's columns
        for name, column in original.get_columns():
            if name == "id":
                continue
            joined_schema.add_column(column)

        # Add the new columns
        for column in new_columns:
            if name == "id":
                continue

            # NOTE: All values in QVAL are Char, so all new columns will be Char type
            new_col_schema = SqlColumnSchema.generated(column=column, type="Char")
            joined_schema.add_column(new_col_schema)

        return joined_schema

    @staticmethod
    def _validate_merge(
        original: SqlTableSchema,
        supp: SqlTableSchema,
        required_original_columns: List[str],
        required_supp_columns: List[str],
        new_columns: List[str],
    ):
        """
        Validates the merge by checking if the original and supp schemas have the required columns.
        """
        for col in required_supp_columns:
            if not supp.has_column(col):
                raise ValueError(f"SUPP MERGE: SUPP schema is missing required column: {col}")

        for col in ["STUDYID", "DOMAIN", "USUBJID"]:
            if not original.has_column(col):
                raise ValueError(f"SUPP MERGE: Original schema is missing required base column: {col}")

        for col in required_original_columns:
            if not original.has_column(col):
                raise ValueError(f"SUPP MERGE: Original schema is missing required column: {col}")

        for col in new_columns:
            if original.has_column(col):
                raise ValueError(f"SUPP MERGE: Column already exists in original table: {col}")

    @staticmethod
    def _get_required_supp_columns(domain: bool) -> List[str]:
        """
        Returns the list of required columns in the supp dataset for the SUPP merge.
        SUPPDM does not require IDVAR or IDVARVAL, as USUBJID if already unique.
        """
        if domain == "DM":
            return ["STUDYID", "RDOMAIN", "USUBJID"]
        else:
            return ["STUDYID", "RDOMAIN", "USUBJID", "IDVAR", "IDVARVAL"]

    @staticmethod
    def _get_required_original_columns(pgi: PostgresQLInterface, supp: SqlTableSchema, domain: bool) -> List[str]:
        """
        Returns the list of required columns in the original dataset for the SUPP merge.
        DM does not require any extra columns as USUBJID if already unique.
        """
        if domain == "DM":
            return []
        else:
            if not supp.has_column("IDVAR"):
                raise ValueError("SUPP MERGE: SUPP schema is missing required column: IDVAR")

            pgi.execute_sql(f"SELECT DISTINCT {supp.get_column_hash("IDVAR")} AS col FROM {supp.hash}")
            result = pgi.fetch_all()
            return [row["col"] for row in result]

    @staticmethod
    def _get_linking_clauses(
        original: SqlTableSchema,
        supp: SqlTableSchema,
        required_original_columns: List[str],
    ) -> List[str]:
        """
        Returns the list of linking clauses for the where clause.
        """
        default_clauses = " AND ".join(
            [
                f"original.{original.get_column_hash("STUDYID")} = supp.{supp.get_column_hash("STUDYID")}",
                f"original.{original.get_column_hash("DOMAIN")} = supp.{supp.get_column_hash("RDOMAIN")}",
                f"original.{original.get_column_hash("USUBJID")} = supp.{supp.get_column_hash("USUBJID")}",
            ]
        )

        if len(required_original_columns) == 0:
            return [default_clauses]

        return [
            f"""{default_clauses}
                AND supp.{supp.get_column_hash("IDVAR")} = '{col}'
                AND original.{original.get_column_hash(col)}::text = supp.{supp.get_column_hash("IDVARVAL")}"""
            for col in required_original_columns
        ]

    @staticmethod
    def _determine_new_columns(pgi: PostgresQLInterface, supp_table: SqlTableSchema) -> List[str]:
        """
        Determines the new columns that will be added to the schema after the join.
        """
        pgi.execute_sql(f"SELECT DISTINCT {supp_table.get_column_hash("QNAM")} AS col FROM {supp_table.hash}")
        result = pgi.fetch_all()
        return [row["col"] for row in result]
