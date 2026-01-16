from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema


class SqlRelationshipMerge:
    @staticmethod
    def perform_join(
        pgi: PostgresQLInterface,
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        domain: str,
        relationship_columns: dict,
        match_keys: list = None,
    ) -> SqlTableSchema:
        """
        1. Filter by match keys of relationship dataset
        2. Filter by RDOMAIN and relationship columns (SUPP-style filtering)
        3. Merge with full outer join and domain suffixes
        """
        try:
            if match_keys is None:
                match_keys = ["STUDYID", "USUBJID"]

            # Validate required columns
            SqlRelationshipMerge._validate_merge(original, relationship_dataset, relationship_columns, match_keys)

            name = f"{original.name}_REL_{domain}"

            # Check if the table already exists
            if pgi.schema.get_table(name) is not None:
                return pgi.schema.get_table(name)

            # Get relationship column names
            column_with_names = relationship_columns.get("column_with_names")
            column_with_values = relationship_columns.get("column_with_values")

            # Check if relationship columns are all empty - if so, do simple outer join
            if SqlRelationshipMerge._has_empty_relationship_columns(
                pgi, relationship_dataset, column_with_names, column_with_values
            ):
                return SqlRelationshipMerge._perform_simple_merge(
                    pgi, original, relationship_dataset, domain, match_keys
                )

            schema = SqlRelationshipMerge._build_merged_schema(
                original, relationship_dataset, domain, name, match_keys, pgi
            )
            pgi.create_table(schema)

            SqlRelationshipMerge._execute_relationship_merge(
                pgi, schema, original, relationship_dataset, domain, column_with_names, column_with_values, match_keys
            )

            return schema

        except ValueError as e:
            raise ValueError(
                f"Relationship merge failed for {original.name} with {relationship_dataset.name}: {str(e)}"
            )
        except Exception as e:
            raise RuntimeError(f"Relationship merge encountered unexpected error for {original.name}: {str(e)}")

    @staticmethod
    def _validate_merge(
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        relationship_columns: dict,
        match_keys: list,
    ):
        """Validate that required columns exist for the relationship merge."""
        # Check match key columns exist in both datasets
        for col in match_keys:
            if not original.has_column(col):
                raise ValueError(f"RELATIONSHIP MERGE: Original schema missing match key column: {col}")
            if not relationship_dataset.has_column(col):
                raise ValueError(f"RELATIONSHIP MERGE: Relationship schema missing match key column: {col}")

        if not relationship_columns:
            raise ValueError("RELATIONSHIP MERGE: relationship_columns parameter is required but was None or empty")

        column_with_names = relationship_columns.get("column_with_names")
        column_with_values = relationship_columns.get("column_with_values")

        if not column_with_names or str(column_with_names).strip() == "":
            raise ValueError(f"RELATIONSHIP MERGE: column_with_names is required but was: {column_with_names}")

        if not column_with_values or str(column_with_values).strip() == "":
            raise ValueError(f"RELATIONSHIP MERGE: column_with_values is required but was: {column_with_values}")

        # Column existence validated in _has_empty_relationship_columns; missing columns trigger simple merge
        if relationship_dataset.has_column("RDOMAIN") and not (
            original.has_column("DOMAIN") or original.has_column("RDOMAIN")
        ):
            raise ValueError(
                "RELATIONSHIP MERGE: Original schema missing DOMAIN or RDOMAIN column when right has RDOMAIN"
            )

    @staticmethod
    def _has_empty_relationship_columns(
        pgi: PostgresQLInterface,
        relationship_dataset: SqlTableSchema,
        column_with_names: str,
        column_with_values: str,
    ) -> bool:
        """Check if all relationship columns are empty."""
        if not column_with_names or not column_with_values:
            return True

        if not relationship_dataset.has_column(column_with_names) or not relationship_dataset.has_column(
            column_with_values
        ):
            return True

        try:
            names_hash = relationship_dataset.get_column_hash(column_with_names)
            values_hash = relationship_dataset.get_column_hash(column_with_values)

            if not names_hash or not values_hash:
                return True

            query = f"""
                SELECT COUNT(*) as count
                FROM {relationship_dataset.hash}
                WHERE TRIM(COALESCE({names_hash}, '')) != '' OR TRIM(COALESCE({values_hash}, '')) != ''
            """

            pgi.execute_sql(query)
            result = pgi.fetch_one()
            return result["count"] == 0
        except Exception:
            return True

    @staticmethod
    def _perform_simple_merge(
        pgi: PostgresQLInterface,
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        domain: str,
        match_keys: list,
    ) -> SqlTableSchema:
        """Perform simple outer join when relationship columns are empty."""
        name = f"{original.name}_REL_{domain}_SIMPLE"

        if pgi.schema.get_table(name) is not None:
            return pgi.schema.get_table(name)

        schema = SqlRelationshipMerge._build_merged_schema(
            original, relationship_dataset, domain, name, match_keys, pgi
        )
        pgi.create_table(schema)

        left_cols = [col.hash for col_name, col in original.get_columns() if col_name != "id"]
        right_cols = []
        target_cols = left_cols.copy()

        for col_name, col in relationship_dataset.get_columns():
            # Only skip columns that are actual match keys (they come from left dataset)
            if col_name == "id" or col_name in match_keys:
                continue

            suffixed_name = f"{col_name}.{domain}"
            if schema.has_column(suffixed_name):
                suffixed_hash = schema.get_column_hash(suffixed_name)
                right_cols.append(f"r.{col.hash} AS {suffixed_hash}")
                target_cols.append(suffixed_hash)

        all_selects = [f"l.{col}" for col in left_cols] + right_cols

        # Build filtered left subquery (apply match key and RDOMAIN filtering)
        filters = []
        match_key_filter = SqlRelationshipMerge._build_match_key_filter(original, relationship_dataset, match_keys)
        if match_key_filter:
            filters.append(match_key_filter)

        rdomain_filter = SqlRelationshipMerge._build_rdomain_filter(original, relationship_dataset)
        if rdomain_filter:
            filters.append(rdomain_filter)

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        filtered_left_subquery = f"SELECT * FROM {original.hash} {where_clause}"

        # Build JOIN condition using match_keys
        join_conditions = []
        for key in match_keys:
            left_hash = original.get_column_hash(key)
            right_hash = relationship_dataset.get_column_hash(key)
            join_conditions.append(f"l.{left_hash} = r.{right_hash}")

        query = f"""
            INSERT INTO {schema.hash} ({', '.join(target_cols)})
            SELECT {', '.join(all_selects)}
            FROM ({filtered_left_subquery}) l
            FULL OUTER JOIN {relationship_dataset.hash} r
                ON {' AND '.join(join_conditions)}
        """

        pgi.execute_sql(query)
        return schema

    @staticmethod
    def _build_merged_schema(
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        domain: str,
        name: str,
        match_keys: list,
        pgi: PostgresQLInterface,
    ) -> SqlTableSchema:
        """Build merged schema with domain suffixes."""
        schema = SqlTableSchema.derived(name, pgi)

        for col_name, column in original.get_columns():
            if col_name == "id":
                continue
            schema.add_column(column)

        for col_name, column in relationship_dataset.get_columns():
            # Only skip columns that are actual match keys (they come from left dataset)
            if col_name == "id" or col_name in match_keys:
                continue

            suffixed_name = f"{col_name}.{domain}"
            if not schema.has_column(suffixed_name):
                new_col_schema = SqlColumnSchema.generated(column=suffixed_name, type=column.type)
                schema.add_column(new_col_schema)

                if not schema.has_column(col_name):
                    schema.add_column(SqlColumnSchema.alias(col_name, new_col_schema))

        return schema

    @staticmethod
    def _execute_relationship_merge(
        pgi: PostgresQLInterface,
        schema: SqlTableSchema,
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        domain: str,
        column_with_names: str,
        column_with_values: str,
        match_keys: list,
    ):
        """Execute relationship merge: filter original dataset, then outer join with first IDVAR as join key."""
        filtered_left_query = SqlRelationshipMerge._build_filtered_left_subquery(
            pgi, original, relationship_dataset, column_with_names, column_with_values, match_keys
        )

        first_idvar = SqlRelationshipMerge._get_first_idvar_value(pgi, relationship_dataset, column_with_names)

        left_columns, right_columns, target_columns = SqlRelationshipMerge._build_select_clauses(
            original, relationship_dataset, schema, domain
        )

        values_hash = relationship_dataset.get_column_hash(column_with_values)

        # Build join conditions from match_keys
        join_conditions = []
        for key in match_keys:
            left_hash = original.get_column_hash(key)
            right_hash = relationship_dataset.get_column_hash(key)
            join_conditions.append(f"l.{left_hash} = r.{right_hash}")

        # Add first IDVAR as additional join key if present
        if first_idvar and original.has_column(first_idvar):
            col_hash = original.get_column_hash(first_idvar)
            join_conditions.append(f"l.{col_hash}::text = r.{values_hash}::text")

        query = f"""
            INSERT INTO {schema.hash} ({', '.join(target_columns)})
            SELECT {', '.join(left_columns + right_columns)}
            FROM ({filtered_left_query}) l
            FULL OUTER JOIN {relationship_dataset.hash} r ON {' AND '.join(join_conditions)}
        """

        pgi.execute_sql(query)

    @staticmethod
    def _build_filtered_left_subquery(
        pgi: PostgresQLInterface,
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        column_with_names: str,
        column_with_values: str,
        match_keys: list,
    ) -> str:
        """Build filtered left subquery: match keys, RDOMAIN, and IDVAR/IDVARVAL filtering."""
        filters = []

        match_key_filter = SqlRelationshipMerge._build_match_key_filter(original, relationship_dataset, match_keys)
        if match_key_filter:
            filters.append(match_key_filter)

        rdomain_filter = SqlRelationshipMerge._build_rdomain_filter(original, relationship_dataset)
        if rdomain_filter:
            filters.append(rdomain_filter)

        idvar_filter = SqlRelationshipMerge._build_idvar_filter(
            pgi, original, relationship_dataset, column_with_names, column_with_values
        )
        if idvar_filter:
            filters.append(idvar_filter)

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        return f"SELECT * FROM {original.hash} {where_clause}"

    @staticmethod
    def _build_match_key_filter(
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        match_keys: list,
    ) -> str:
        """Filter original dataset to only rows where match key tuples exist in relationship dataset."""
        # Verify all match keys exist in both datasets
        for key in match_keys:
            if not original.has_column(key) or not relationship_dataset.has_column(key):
                return ""

        # Build column lists for the IN clause
        original_cols = [f"{original.hash}.{original.get_column_hash(key)}" for key in match_keys]
        rel_cols = [relationship_dataset.get_column_hash(key) for key in match_keys]

        # Single column optimization (no tuple needed)
        if len(match_keys) == 1:
            return f"""
                {original_cols[0]} IN (
                    SELECT {rel_cols[0]}
                    FROM {relationship_dataset.hash}
                )
            """

        # Multiple columns - use tuple syntax
        return f"""
            ({', '.join(original_cols)}) IN (
                SELECT {', '.join(rel_cols)}
                FROM {relationship_dataset.hash}
            )
        """

    @staticmethod
    def _build_rdomain_filter(
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
    ) -> str:
        """Filter by RDOMAIN: keep only rows where DOMAIN matches RDOMAIN values."""
        if not original.has_column("DOMAIN") or not relationship_dataset.has_column("RDOMAIN"):
            return ""

        domain_hash = original.get_column_hash("DOMAIN")
        rdomain_hash = relationship_dataset.get_column_hash("RDOMAIN")

        return f"""
            {original.hash}.{domain_hash} IN (
                SELECT DISTINCT {rdomain_hash} FROM {relationship_dataset.hash}
            )
        """

    @staticmethod
    def _build_idvar_filter(
        pgi: PostgresQLInterface,
        original: SqlTableSchema,
        relationship_dataset: SqlTableSchema,
        column_with_names: str,
        column_with_values: str,
    ) -> str:
        """
        Filter by IDVAR/IDVARVAL: only filter columns mentioned in IDVAR column.
        Example: IDVAR=["AESEQ","AESEV"], IDVARVAL=["1","MILD"] → AESEQ IN ('1') AND AESEV IN ('MILD')
        """
        if not relationship_dataset.has_column(column_with_names) or not relationship_dataset.has_column(
            column_with_values
        ):
            return ""

        names_hash = relationship_dataset.get_column_hash(column_with_names)
        values_hash = relationship_dataset.get_column_hash(column_with_values)

        query = f"""
            SELECT DISTINCT {names_hash} as idvar_col
            FROM {relationship_dataset.hash}
            WHERE TRIM(COALESCE({names_hash}, '')) != ''
        """
        pgi.execute_sql(query)
        idvar_columns = pgi.fetch_all()

        if not idvar_columns:
            return ""

        column_filters = []
        for row in idvar_columns:
            idvar_col = row.get("idvar_col")
            if not idvar_col or not original.has_column(idvar_col):
                continue

            col_hash = original.get_column_hash(idvar_col)

            column_filter = f"""
                {original.hash}.{col_hash}::text IN (
                    SELECT r.{values_hash}::text
                    FROM {relationship_dataset.hash} r
                    WHERE r.{names_hash} = '{idvar_col}'
                    AND TRIM(COALESCE(r.{values_hash}, '')) != ''
                )
            """
            column_filters.append(column_filter)

        if column_filters:
            return f"({' AND '.join(column_filters)})"

        return ""

    @staticmethod
    def _get_first_idvar_value(
        pgi: PostgresQLInterface,
        relationship_dataset: SqlTableSchema,
        column_with_names: str,
    ) -> str:
        """Get first non-empty IDVAR value for dynamic join."""
        if not relationship_dataset.has_column(column_with_names):
            return None

        names_hash = relationship_dataset.get_column_hash(column_with_names)

        query = f"""
            SELECT {names_hash} as first_col_name
            FROM {relationship_dataset.hash}
            WHERE TRIM(COALESCE({names_hash}, '')) != ''
            LIMIT 1
        """

        pgi.execute_sql(query)
        result = pgi.fetch_one()

        if result and result.get("first_col_name"):
            return result["first_col_name"]

        return None

    @staticmethod
    def _build_select_clauses(original, relationship_dataset, schema, domain):
        """Build select clauses for both tables with domain suffixes."""
        left_columns = []
        right_columns = []
        target_columns = []

        for col_name, col in original.get_columns():
            if col_name == "id":
                continue
            left_columns.append(f"l.{col.hash}")
            target_columns.append(col.hash)

        for col_name, col in relationship_dataset.get_columns():
            if col_name in ["id", "STUDYID", "USUBJID"]:
                continue

            suffixed_name = f"{col_name}.{domain}"
            if schema.has_column(suffixed_name):
                suffixed_hash = schema.get_column_hash(suffixed_name)
                right_columns.append(f"r.{col.hash} AS {suffixed_hash}")
                target_columns.append(suffixed_hash)

        return left_columns, right_columns, target_columns
