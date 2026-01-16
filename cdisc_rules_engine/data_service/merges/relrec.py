from typing import List, Dict

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema


class SqlRelrecMerge:
    @staticmethod
    def perform_join(
        pgi: PostgresQLInterface,
        original: SqlTableSchema,
        relrec: SqlTableSchema,
        domain: str,
        wildcard: str = "__",
    ) -> SqlTableSchema:
        """
        Perform a RELREC merge operation on the datasets.

        This merge creates relationships between records based on the RELREC dataset:
        1. Filter RELREC records for the target domain
        2. Find all related domains through relationship records
        3. For each relationship, join the original dataset with related datasets
        4. Apply wildcard column renaming (e.g., ECSTDY -> RELREC.__STDY)
        5. Union all results
        """
        # Validate required columns exist
        SqlRelrecMerge._validate_merge(original, relrec, domain)

        name = f"{original.name}_RELREC_{wildcard}"

        # Check if the table already exists
        if pgi.schema.get_table(name) is not None:
            return pgi.schema.get_table(name)

        # Get relationship records for the target domain
        relationships = SqlRelrecMerge._filter_relrec_for_domain(pgi, relrec, domain)

        if not relationships:
            return original

        # Build the merged schema with renamed columns
        schema = SqlRelrecMerge._build_merged_schema(pgi, name, original, relationships, wildcard)
        pgi.create_table(schema)

        # Process each relationship and union results
        SqlRelrecMerge._process_relationship_records(pgi, schema, original, relationships, wildcard)

        return schema

    @staticmethod
    def _validate_merge(
        original: SqlTableSchema,
        relrec: SqlTableSchema,
        domain: str,
    ):
        """
        Validate that the required columns exist for the RELREC merge.
        """
        required_original_columns = ["STUDYID", "USUBJID"]
        required_relrec_columns = ["STUDYID", "USUBJID", "RELID", "RDOMAIN"]

        for col in required_original_columns:
            if not original.has_column(col):
                raise ValueError(f"RELREC MERGE: Original schema is missing required column: {col}")

        for col in required_relrec_columns:
            if not relrec.has_column(col):
                raise ValueError(f"RELREC MERGE: RELREC schema is missing required column: {col}")

    @staticmethod
    def _filter_relrec_for_domain(
        pgi: PostgresQLInterface,
        relrec: SqlTableSchema,
        domain: str,
    ) -> List[Dict]:
        """
        Filter RELREC records for the target domain and create relationship pairs.

        This is the SQL equivalent of the Python filter_relrec_for_domain function.
        """
        query = f"""
            SELECT
                left_rel.{relrec.get_column_hash("STUDYID")} as studyid,
                left_rel.{relrec.get_column_hash("USUBJID")} as usubjid,
                left_rel.{relrec.get_column_hash("RELID")} as relid,
                left_rel.{relrec.get_column_hash("RDOMAIN")} as rdomain_left,
                COALESCE(left_rel.{relrec.get_column_hash("IDVAR")}, '') as idvar_left,
                COALESCE(left_rel.{relrec.get_column_hash("IDVARVAL")}, '') as idvarval_left,
                right_rel.{relrec.get_column_hash("RDOMAIN")} as rdomain_right,
                COALESCE(right_rel.{relrec.get_column_hash("IDVAR")}, '') as idvar_right,
                COALESCE(right_rel.{relrec.get_column_hash("IDVARVAL")}, '') as idvarval_right
            FROM {relrec.hash} left_rel
            INNER JOIN {relrec.hash} right_rel
                ON left_rel.{relrec.get_column_hash("STUDYID")} = right_rel.{relrec.get_column_hash("STUDYID")}
                AND left_rel.{relrec.get_column_hash("USUBJID")} = right_rel.{relrec.get_column_hash("USUBJID")}
                AND left_rel.{relrec.get_column_hash("RELID")} = right_rel.{relrec.get_column_hash("RELID")}
            WHERE left_rel.{relrec.get_column_hash("RDOMAIN")} = '{domain}'
                AND right_rel.{relrec.get_column_hash("RDOMAIN")} != '{domain}'
        """

        pgi.execute_sql(query)
        return pgi.fetch_all()

    @staticmethod
    def _apply_wildcard_renaming(
        pgi: PostgresQLInterface,
        right_table: SqlTableSchema,
        domain: str,
        wildcard: str,
    ) -> Dict[str, str]:
        """
        Apply wildcard renaming to columns from related domains.

        This is a simplified version of the add_variable_wildcards function.
        For domain-specific columns like ECSTDY, ECENDY, etc., replace domain prefix with wildcard.
        """
        renamed_columns = {}

        for col_name, column in right_table.get_columns():
            if col_name == "id":
                continue

            column_upper = col_name.upper()
            domain_upper = domain.upper()

            if column_upper.startswith(domain_upper):
                # Check if this looks like a domain-specific variable
                suffix = column_upper[len(domain_upper) :]
                # Common patterns: STDY, ENDY, DY, TM, etc.
                if suffix in ["STDY", "ENDY", "DY", "TM", "DTC", "SEQ"] or suffix.startswith("_"):
                    new_name = f"RELREC.{wildcard}{suffix}"
                    renamed_columns[col_name] = new_name
                else:
                    # Keep original name but with RELREC prefix
                    renamed_columns[col_name] = f"RELREC.{col_name}"
            else:
                # Keep original name but with RELREC prefix
                renamed_columns[col_name] = f"RELREC.{col_name}"

        return renamed_columns

    @staticmethod
    def _build_merged_schema(
        pgi: PostgresQLInterface,
        name: str,
        original: SqlTableSchema,
        relationships: List[Dict],
        wildcard: str,
    ) -> SqlTableSchema:
        """
        Build the output schema including original columns and renamed relationship columns.
        """
        schema = SqlTableSchema.derived(name, pgi)

        # Add all original columns including id
        for _, column in original.get_columns():
            schema.add_column(column)

        # For each unique right domain, determine what columns to add
        right_domains = set(rel["rdomain_right"] for rel in relationships)

        for right_domain in right_domains:
            # Try to find the table for this domain in the schema
            right_table = None
            for table_name, table_schema in pgi.schema.get_tables():
                if table_name.lower().startswith(right_domain.lower()):
                    right_table = table_schema
                    break

            if right_table:
                # Get column mappings for this domain
                column_mappings = SqlRelrecMerge._apply_wildcard_renaming(pgi, right_table, right_domain, wildcard)
                SqlRelrecMerge._add_mapped_columns(schema, right_table, column_mappings)

        return schema

    @staticmethod
    def _add_mapped_columns(
        schema: SqlTableSchema,
        right_table: SqlTableSchema,
        column_mappings: Dict[str, str],
    ):
        """Add mapped columns to the schema with proper types."""
        for original_col, renamed_col in column_mappings.items():
            if not schema.has_column(renamed_col):
                # Get original column type
                original_column = None
                for col_name, col in right_table.get_columns():
                    if col_name == original_col:
                        original_column = col
                        break

                if original_column:
                    new_col_schema = SqlColumnSchema.generated(column=renamed_col, type=original_column.type)
                    schema.add_column(new_col_schema)

    @staticmethod
    def _process_relationship_records(
        pgi: PostgresQLInterface,
        schema: SqlTableSchema,
        original: SqlTableSchema,
        relationships: List[Dict],
        wildcard: str,
    ):
        """
        Process each relationship record and populate the result table.
        """
        if not relationships:
            SqlRelrecMerge._copy_original_data_only(pgi, schema, original)
            return

        # Group relationships by right domain and process
        domain_relationships = SqlRelrecMerge._group_relationships_by_domain(relationships)
        queries = SqlRelrecMerge._build_domain_queries(pgi, schema, original, domain_relationships, wildcard)

        # Execute all queries
        if queries:
            pgi.execute_many(queries)

    @staticmethod
    def _copy_original_data_only(pgi: PostgresQLInterface, schema: SqlTableSchema, original: SqlTableSchema):
        """Copy original data when no relationships exist."""
        source_columns = [col.hash for col_name, col in original.get_columns() if col_name != "id"]
        target_columns = [col.hash for col_name, col in schema.get_columns() if col_name != "id"]
        query = f"""
            INSERT INTO {schema.hash} ({', '.join(target_columns)})
            SELECT {', '.join(source_columns)}
            FROM {original.hash}
            ORDER BY id
        """
        pgi.execute_sql(query)

    @staticmethod
    def _group_relationships_by_domain(relationships: List[Dict]) -> Dict[str, List[Dict]]:
        """Group relationships by right domain."""
        domain_relationships = {}
        for rel in relationships:
            right_domain = rel["rdomain_right"]
            if right_domain not in domain_relationships:
                domain_relationships[right_domain] = []
            domain_relationships[right_domain].append(rel)
        return domain_relationships

    @staticmethod
    def _build_domain_queries(
        pgi: PostgresQLInterface,
        schema: SqlTableSchema,
        original: SqlTableSchema,
        domain_relationships: Dict[str, List[Dict]],
        wildcard: str,
    ) -> List[str]:
        """Build queries for all domain relationships."""
        queries = []
        for right_domain, domain_rels in domain_relationships.items():
            right_table = pgi.schema.get_table(right_domain.lower())
            if right_table:
                query = SqlRelrecMerge._build_single_domain_query(
                    pgi, schema, original, right_table, right_domain, domain_rels, wildcard
                )
                if query:
                    queries.append(query)
        return queries

    @staticmethod
    def _build_single_domain_query(
        pgi: PostgresQLInterface,
        schema: SqlTableSchema,
        original: SqlTableSchema,
        right_table: SqlTableSchema,
        right_domain: str,
        domain_rels: List[Dict],
        wildcard: str,
    ) -> str:
        """Build query for a single domain's relationships."""
        column_mappings = SqlRelrecMerge._apply_wildcard_renaming(pgi, right_table, right_domain, wildcard)

        # Build select clauses and target columns
        original_selects, right_selects, target_columns = SqlRelrecMerge._build_select_clauses(
            original, right_table, schema, column_mappings
        )

        # Build union parts for all relationships
        union_parts = SqlRelrecMerge._build_union_parts(
            original, right_table, domain_rels, original_selects, right_selects
        )

        if not union_parts:
            return None

        id_col_hash = original.get_column_hash("id")

        return f"""
            INSERT INTO {schema.hash} ({', '.join(target_columns)})
            SELECT {', '.join(target_columns)} FROM (
                {' UNION '.join(union_parts)}
            ) AS merged_data
            ORDER BY {id_col_hash}
        """

    @staticmethod
    def _build_select_clauses(original, right_table, schema, column_mappings):
        """Build select clauses and target columns."""
        original_selects = []
        right_selects = []
        target_columns = []

        # Original table columns
        for col_name, col in original.get_columns():
            original_selects.append(f"o.{col.hash}")
            if col_name != "id":
                target_columns.append(col.hash)

        # Right table columns (renamed)
        for original_col, renamed_col in column_mappings.items():
            if schema.has_column(renamed_col):
                right_col_hash = right_table.get_column_hash(original_col)
                renamed_col_hash = schema.get_column_hash(renamed_col)
                right_selects.append(f"r.{right_col_hash} AS {renamed_col_hash}")
                target_columns.append(renamed_col_hash)

        return original_selects, right_selects, target_columns

    @staticmethod
    def _build_union_parts(original, right_table, domain_rels, original_selects, right_selects):
        """Build union parts for all relationships in a domain."""
        union_parts = []
        for rel in domain_rels:
            join_conditions = [
                f"o.{original.get_column_hash('STUDYID')} = r.{right_table.get_column_hash('STUDYID')}",
                f"o.{original.get_column_hash('USUBJID')} = r.{right_table.get_column_hash('USUBJID')}",
            ]

            where_filters = [f"UPPER(o.{original.get_column_hash('STUDYID')}::text) = UPPER('{rel['studyid']}')"]

            if rel.get("usubjid") and rel["usubjid"].strip():
                where_filters.append(
                    f"UPPER(o.{original.get_column_hash('USUBJID')}::text) = UPPER('{rel['usubjid']}')"
                )

            if rel.get("idvarval_left") and rel["idvarval_left"]:
                if original.has_column(rel["idvar_left"]):
                    where_filters.append(
                        f"o.{original.get_column_hash(rel['idvar_left'])}::text = '{rel['idvarval_left']}'"
                    )
                if right_table.has_column(rel.get("idvar_right", "")):
                    join_conditions.append(
                        f"r.{right_table.get_column_hash(rel['idvar_right'])}::text = '{rel['idvarval_right']}'"
                    )
            else:
                if rel.get("idvar_left") and rel.get("idvar_right"):
                    if original.has_column(rel["idvar_left"]) and right_table.has_column(rel["idvar_right"]):
                        left_hash = original.get_column_hash(rel["idvar_left"])
                        right_hash = right_table.get_column_hash(rel["idvar_right"])
                        join_conditions.append(f"o.{left_hash}::text = r.{right_hash}::text")

            where_clause = f"WHERE {' AND '.join(where_filters)}"

            union_part = f"""
                SELECT {', '.join(original_selects + right_selects)}
                FROM {original.hash} o
                INNER JOIN {right_table.hash} r ON {' AND '.join(join_conditions)}
                {where_clause}
            """
            union_parts.append(union_part)

        return union_parts
