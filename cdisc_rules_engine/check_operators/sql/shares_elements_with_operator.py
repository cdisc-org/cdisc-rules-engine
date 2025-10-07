from .base_sql_operator import BaseSqlOperator


class SharesElementsWithOperator(BaseSqlOperator):
    """Unified operator for checking if values share elements with different comparison modes."""

    def __init__(self, data, operation_type="no_elements"):
        """Initialize the operator with a specific operation type.

        Args:
            data: The data service and other initialization parameters
            operation_type: One of 'no_elements', 'at_least_one', or 'exactly_one'
        """
        super().__init__(data)
        self.operation_type = operation_type

    def execute_operator(self, other_value):
        """
        Checks if values share elements according to the operation type.

        NOTE: This operator performs DATASET-LEVEL analysis, not row-level analysis.
        The comparison is done once at the dataset level (e.g., comparing all unique values
        in a target column with all unique values in a comparator column), but returns
        a consistent truth series for each row to maintain compatibility with the SQL
        engine execution framework.

        Operation types:
        - 'no_elements': Returns True for each row if target and comparator share no elements at dataset level
        - 'at_least_one': Returns True for each row if target and comparator share at least one element at dataset level
        - 'exactly_one': Returns True for each row if target and comparator share exactly one element at dataset level

        The dataset-level comparison converts both target and comparator values to sets:
        - If the value is a column, uses all unique non-empty values from that column
        - If the value is a collection operation variable, uses all values from the collection
        - If the value is a single element/constant, treats it as a single-element set

        Both target and comparator can be:
        - Column names (dataset-level: all unique non-empty values in the column)
        - Operation variables (constant or collection type)
        - Literal single values (treated as single-element sets)

        Returns:
            pd.Series: A pandas Series of booleans with the same value for each row in the dataset
                      (the dataset-level comparison result replicated for all rows)
        """
        target = other_value.get("target")
        comparator = other_value.get("comparator")

        if target is None or comparator is None:
            raise ValueError("Missing required parameters: target or comparator")

        target = self.replace_prefix(target) if isinstance(target, str) else target
        comparator = self.replace_prefix(comparator) if isinstance(comparator, str) else comparator

        cache_key = f"{target}_shares_elements_{self.operation_type}_{comparator}"

        def sql():
            return self._build_shares_elements_query(target, comparator)

        return self._do_check_operator(cache_key, sql)

    def _is_collection_variable(self, value):
        """Check if a value is a collection-type operation variable."""
        return (
            isinstance(value, str)
            and value in self.operation_variables
            and self.operation_variables[value].type == "collection"
        )

    def _is_constant_variable(self, value):
        """Check if a value is a constant-type operation variable."""
        return (
            isinstance(value, str)
            and value in self.operation_variables
            and self.operation_variables[value].type == "constant"
        )

    def _collection_value_not_empty_sql(self, alias_prefix=""):
        """Generate SQL condition to filter out empty collection values."""
        value_col = f"{alias_prefix}value" if alias_prefix else "value"
        return f"{value_col} IS NOT NULL AND {value_col} != ''"

    def _build_shares_elements_query(self, target, comparator):
        """Build the appropriate SQL query based on value types."""
        # Determine value types for routing
        target_is_collection = self._is_collection_variable(target)
        comparator_is_collection = self._is_collection_variable(comparator)
        target_is_constant = self._is_constant_variable(target)
        comparator_is_constant = self._is_constant_variable(comparator)
        target_is_column = (
            isinstance(target, str)
            and not target_is_collection
            and not target_is_constant
            and self.sql_data_service.pgi.schema.column_exists(self.table_id, target)
        )
        comparator_is_column = (
            isinstance(comparator, str)
            and not comparator_is_collection
            and not comparator_is_constant
            and self.sql_data_service.pgi.schema.column_exists(self.table_id, comparator)
        )

        # Handle simple vs simple (constants, literals, or constant variables)
        target_is_simple = (
            target_is_constant
            or not isinstance(target, str)
            or (isinstance(target, str) and not target_is_collection and not target_is_column)
        )
        comparator_is_simple = (
            comparator_is_constant
            or not isinstance(comparator, str)
            or (isinstance(comparator, str) and not comparator_is_collection and not comparator_is_column)
        )

        if target_is_simple and comparator_is_simple:
            return self._build_simple_vs_simple_query(target, comparator)

        # Handle collection vs collection
        elif target_is_collection and comparator_is_collection:
            return self._build_collection_vs_collection_query(target, comparator)

        # Handle collection vs column
        elif target_is_collection and comparator_is_column:
            return self._build_collection_vs_column_query(target, comparator)

        elif target_is_column and comparator_is_collection:
            return self._build_collection_vs_column_query(comparator, target)

        # Handle column vs column comparison (both are column names)
        elif target_is_column and comparator_is_column:
            return self._build_column_vs_column_query(target, comparator)

        # Handle collection vs simple value (constant or literal)
        elif target_is_collection and comparator_is_simple:
            comparator_sql = self._sql(comparator)
            comparator_empty_sql = self._is_empty_sql(comparator, alias=False)
            return self._build_collection_vs_value_query(
                target, comparator_sql, comparator_empty_sql, target_is_collection=True
            )

        elif target_is_simple and comparator_is_collection:
            target_sql = self._sql(target)
            target_empty_sql = self._is_empty_sql(target, alias=False)
            return self._build_collection_vs_value_query(
                comparator, target_sql, target_empty_sql, target_is_collection=False
            )

        else:
            raise ValueError(f"Unsupported comparison types: target={target}, comparator={comparator}")

    def _build_collection_vs_value_query(self, collection_var, value_sql, value_empty_sql, target_is_collection):
        """Build query for collection vs single value comparison."""
        collection_sql = self._collection_sql(collection_var)
        collection_not_empty = self._collection_value_not_empty_sql("collection_values.")

        if self.operation_type == "no_elements":
            return f"""
            NOT EXISTS (
                SELECT 1 FROM {collection_sql} AS collection_values(value)
                CROSS JOIN {self._table_sql()} AS co
                WHERE {collection_not_empty}
                AND NOT ({value_empty_sql})
                AND collection_values.value = {value_sql}
            )
            """
        elif self.operation_type == "at_least_one":
            return f"""
            EXISTS (
                SELECT 1 FROM {collection_sql} AS collection_values(value)
                CROSS JOIN {self._table_sql()} AS co
                WHERE {collection_not_empty}
                AND NOT ({value_empty_sql})
                AND collection_values.value = {value_sql}
            )
            """
        elif self.operation_type == "exactly_one":
            return f"""
            (
                SELECT COUNT(DISTINCT collection_values.value)
                FROM {collection_sql} AS collection_values(value)
                CROSS JOIN {self._table_sql()} AS co
                WHERE {collection_not_empty}
                AND NOT ({value_empty_sql})
                AND collection_values.value = {value_sql}
            ) = 1
            """

    def _build_simple_vs_simple_query(self, target, comparator):
        """Build query for simple value vs simple value comparison."""
        target_sql = self._sql(target)
        comparator_sql = self._sql(comparator)
        target_empty_sql = self._is_empty_sql(target, alias=False)
        comparator_empty_sql = self._is_empty_sql(comparator, alias=False)

        if self.operation_type == "no_elements":
            return f"""
            NOT EXISTS (
                SELECT 1 FROM {self._table_sql()} AS co
                WHERE NOT ({target_empty_sql})
                AND NOT ({comparator_empty_sql})
                AND {target_sql} = {comparator_sql}
            )
            """
        elif self.operation_type == "at_least_one":
            return f"""
            EXISTS (
                SELECT 1 FROM {self._table_sql()} AS co
                WHERE NOT ({target_empty_sql})
                AND NOT ({comparator_empty_sql})
                AND {target_sql} = {comparator_sql}
            )
            """
        elif self.operation_type == "exactly_one":
            # For simple values, exactly_one is the same as at_least_one
            # since there can only be 0 or 1 shared element when comparing single values
            return f"""
            EXISTS (
                SELECT 1 FROM {self._table_sql()} AS co
                WHERE NOT ({target_empty_sql})
                AND NOT ({comparator_empty_sql})
                AND {target_sql} = {comparator_sql}
            )
            """

    def _build_collection_vs_collection_query(self, target_var, comparator_var):
        """Build query for collection vs collection comparison using base class methods."""
        target_collection_sql = self._collection_sql(target_var)
        comparator_collection_sql = self._collection_sql(comparator_var)
        target_not_empty = self._collection_value_not_empty_sql("target_values.")
        comparator_not_empty = self._collection_value_not_empty_sql("comparator_values.")

        if self.operation_type == "no_elements":
            return f"""
            NOT EXISTS (
                SELECT 1 FROM {target_collection_sql} AS target_values(value)
                JOIN {comparator_collection_sql} AS comparator_values(value)
                ON target_values.value = comparator_values.value
                WHERE {target_not_empty} AND {comparator_not_empty}
            )
            """
        elif self.operation_type == "at_least_one":
            return f"""
            EXISTS (
                SELECT 1 FROM {target_collection_sql} AS target_values(value)
                JOIN {comparator_collection_sql} AS comparator_values(value)
                ON target_values.value = comparator_values.value
                WHERE {target_not_empty} AND {comparator_not_empty}
            )
            """
        elif self.operation_type == "exactly_one":
            return f"""
            (
                SELECT COUNT(DISTINCT target_values.value)
                FROM {target_collection_sql} AS target_values(value)
                JOIN {comparator_collection_sql} AS comparator_values(value)
                ON target_values.value = comparator_values.value
                WHERE {target_not_empty} AND {comparator_not_empty}
            ) = 1
            """

    def _build_column_vs_column_query(self, target, comparator):
        """Build query for column vs column comparison.

        When comparing two columns, we need to check how many distinct values
        appear in both columns across all rows.
        """
        target_sql = self._sql(target)
        comparator_sql = self._sql(comparator)
        target_empty_sql = self._is_empty_sql(target, alias=True)
        comparator_empty_sql = self._is_empty_sql(comparator, alias=True)

        if self.operation_type == "no_elements":
            return f"""
            NOT EXISTS (
                SELECT 1 FROM {self._table_sql()} AS target_tbl
                CROSS JOIN {self._table_sql()} AS comparator_tbl
                WHERE NOT ({target_empty_sql.replace('co.', 'target_tbl.')})
                AND NOT ({comparator_empty_sql.replace('co.', 'comparator_tbl.')})
                AND {target_sql.replace('co.', 'target_tbl.')} = {comparator_sql.replace('co.', 'comparator_tbl.')}
            )
            """
        elif self.operation_type == "at_least_one":
            return f"""
            EXISTS (
                SELECT 1 FROM {self._table_sql()} AS target_tbl
                CROSS JOIN {self._table_sql()} AS comparator_tbl
                WHERE NOT ({target_empty_sql.replace('co.', 'target_tbl.')})
                AND NOT ({comparator_empty_sql.replace('co.', 'comparator_tbl.')})
                AND {target_sql.replace('co.', 'target_tbl.')} = {comparator_sql.replace('co.', 'comparator_tbl.')}
            )
            """
        elif self.operation_type == "exactly_one":
            return f"""
            (
                SELECT COUNT(DISTINCT target_tbl.{target})
                FROM {self._table_sql()} AS target_tbl
                CROSS JOIN {self._table_sql()} AS comparator_tbl
                WHERE NOT ({target_empty_sql.replace('co.', 'target_tbl.')})
                AND NOT ({comparator_empty_sql.replace('co.', 'comparator_tbl.')})
                AND {target_sql.replace('co.', 'target_tbl.')} = {comparator_sql.replace('co.', 'comparator_tbl.')}
            ) = 1
            """

    def _build_collection_vs_column_query(self, collection_var, column):
        """Build query for collection vs column comparison."""
        collection_sql = self._collection_sql(collection_var)
        column_sql = self._sql(column)
        collection_not_empty = self._collection_value_not_empty_sql("collection_values.")
        column_empty_sql = self._is_empty_sql(column, alias=True)

        if self.operation_type == "no_elements":
            return f"""
            NOT EXISTS (
                SELECT 1 FROM {collection_sql} AS collection_values(value)
                CROSS JOIN {self._table_sql()} AS co
                WHERE {collection_not_empty}
                AND NOT ({column_empty_sql})
                AND collection_values.value = {column_sql}
            )
            """
        elif self.operation_type == "at_least_one":
            return f"""
            EXISTS (
                SELECT 1 FROM {collection_sql} AS collection_values(value)
                CROSS JOIN {self._table_sql()} AS co
                WHERE {collection_not_empty}
                AND NOT ({column_empty_sql})
                AND collection_values.value = {column_sql}
            )
            """
        elif self.operation_type == "exactly_one":
            return f"""
            (
                SELECT COUNT(DISTINCT collection_values.value)
                FROM {collection_sql} AS collection_values(value)
                CROSS JOIN {self._table_sql()} AS co
                WHERE {collection_not_empty}
                AND NOT ({column_empty_sql})
                AND collection_values.value = {column_sql}
            ) = 1
            """
