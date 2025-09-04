from typing import List, Literal, Tuple

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema


class SqlJoinMerge:
    @staticmethod
    def perform_join(
        pgi: PostgresQLInterface,
        left: SqlTableSchema,
        right: SqlTableSchema,
        pivot_left: list[str],
        pivot_right: list[str],
        type: Literal["INNER", "LEFT", "RIGHT", "FULL OUTER"] = "INNER",
    ) -> SqlTableSchema:
        """Perform a join operation on two SQL table schemas."""
        if len(pivot_left) != len(pivot_right):
            raise ValueError("Pivot columns must have the same length.")

        # Ensure everything is lowercase for consistency
        pivot_left = [col.lower() for col in pivot_left]
        pivot_right = [col.lower() for col in pivot_right]

        # Build the join condition
        join_conditions = []
        for l_var, r_var in zip(pivot_left, pivot_right):
            left_col_hash = left.get_column_hash(l_var)
            right_col_hash = right.get_column_hash(r_var)
            if left_col_hash is None or right_col_hash is None:
                raise ValueError(f"Column {l_var} or {r_var} not found in the respective schemas.")
            join_conditions.append(f"l.{left_col_hash} = r.{right_col_hash}")

        name = f"{left.name}_{type}_{right.name}_ON_{'_'.join(join_conditions)}"

        # Check if the table already exists
        if pgi.schema.get_table(name) is not None:
            return pgi.schema.get_table(name)

        # Build the new schema
        schema, left_columns, right_columns = SqlJoinMerge._join_schemas(
            name=name,
            left=left,
            right=right,
            pivot_left=pivot_left,
            pivot_right=pivot_right,
        )

        pgi.create_table(schema)

        selected_left_columns = [f"l.{old_hash} AS {new_hash}" for old_hash, new_hash in left_columns]
        selected_right_columns = [f"r.{old_hash} AS {new_hash}" for old_hash, new_hash in right_columns]
        target_columns = [new_hash for _, new_hash in (left_columns + right_columns)]

        join_condition = " AND ".join(join_conditions)
        query = f"""
            INSERT INTO {schema.hash} ({', '.join(target_columns)})
                SELECT
                    {', '.join(selected_left_columns)}
                    ,
                    {', '.join(selected_right_columns)}
                FROM {left.hash} l
                {type} JOIN {right.hash} r ON {join_condition}
        """

        pgi.execute_sql(query)

        return schema

    @staticmethod
    def _join_schemas(
        name: str,
        left: SqlTableSchema,
        right: SqlTableSchema,
        pivot_left: list[str],
        pivot_right: list[str],
    ) -> Tuple[SqlTableSchema, List[Tuple[str, str]], List[Tuple[str, str]]]:
        """Join two SQL table schemas based on specified pivot columns."""
        if len(pivot_left) != len(pivot_right):
            raise ValueError("Pivot columns must have the same length.")

        joined_schema = SqlTableSchema.from_join(name)
        left_output_columns = []
        right_output_columns = []

        # Add all of the left table's columns
        for name, column in left.get_columns():
            if name == "id":
                continue
            joined_schema.add_column(column)
            left_output_columns.append((column.hash, column.hash))

        # Add all of the non-pivot columns from the left table
        for name, column in right.get_columns():
            if name == "id":
                continue
            if name in pivot_right:
                continue
            # TODO: We may want to require that all merged variables are referenced by a unique name to avoid clashes
            # new_column_name = f"{left.name}.{name}"
            new_column_name = name
            if joined_schema.get_column(name) is not None:
                # Skipping duplicated column
                continue

            new_col_schema = SqlColumnSchema.generated(column=new_column_name, type=column.type)
            joined_schema.add_column(new_col_schema)
            right_output_columns.append((column.hash, new_col_schema.hash))

        return joined_schema, left_output_columns, right_output_columns
