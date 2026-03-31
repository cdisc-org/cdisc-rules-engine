from .base_sql_operator import BaseSqlOperator


class IsOrderedSetOperator(BaseSqlOperator):
    """
    True if the dataset rows are in ascending order of the values within `name`, grouped by the values within `value`.
    """

    def __init__(self, data, invert=False):
        super().__init__(data)
        self.invert = invert

    def execute_operator(self, other_value):
        name = self.replace_prefix(other_value.get("target"))
        values = other_value.get("comparator")
        value_is_literal = other_value.get("value_is_literal", False)

        if not isinstance(values, list):
            values = [values]

        if not value_is_literal:
            values = [self.replace_prefix(value) for value in values]

        def sql():
            table_hash = self.sql_data_service.pgi.schema.get_table_hash(self.table_id)
            name_hash = self.sql_data_service.pgi.schema.get_column_hash(self.table_id, name)

            partition_cols = []
            for value in values:
                if not value_is_literal:
                    partition_cols.append(self.sql_data_service.pgi.schema.get_column_hash(self.table_id, value))
                else:
                    partition_cols.append(self._constant_sql(value))

            partition_sql = ", ".join(partition_cols) if partition_cols else "NULL"

            group_match_sql = (
                " AND ".join([f"t_all.{col} IS NOT DISTINCT FROM t_bad.{col}" for col in partition_cols])
                if partition_cols
                else "TRUE"
            )

            compare_logic = """
                (sub.prev IS NOT NULL AND sub.curr IS NOT NULL AND (
                    CASE
                        WHEN sub.prev ~ '^[0-9]+(\\.[0-9]+)?$' AND sub.curr ~ '^[0-9]+(\\.[0-9]+)?$'
                        THEN CAST(sub.prev AS NUMERIC) > CAST(sub.curr AS NUMERIC)
                        ELSE sub.prev > sub.curr
                    END
                ))
            """

            dataset_is_ordered = f"""
                ctid NOT IN (
                    SELECT t_all.ctid
                    FROM {table_hash} t_all
                    INNER JOIN (
                        SELECT DISTINCT {partition_sql}
                        FROM (
                            SELECT
                                {partition_sql},
                                {name_hash} AS curr,
                                LAG({name_hash}) OVER (PARTITION BY {partition_sql} ORDER BY ctid) AS prev
                            FROM {table_hash}
                        ) sub
                        WHERE {compare_logic}
                    ) t_bad ON {group_match_sql}
                )
            """
            if self.invert:
                return f"""
                CASE
                    WHEN {self._is_empty_sql(name)} THEN FALSE
                    ELSE NOT ({dataset_is_ordered})
                END
                """
            else:
                return f"""
                CASE
                    WHEN {self._is_empty_sql(name)} THEN FALSE
                    ELSE {dataset_is_ordered}
                END
                """

        return self._do_check_operator(sql)
