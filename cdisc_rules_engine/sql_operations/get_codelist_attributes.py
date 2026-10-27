from typing import Optional, Tuple

from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation

_COLUMN_MAP = {
    "Standard Type": "standard_type",
    "Version Date": "version_date",
    "Term CCODE": "item_code",
    "Codelist Code": "codelist_code",
    "Extensible": "extensible",
    "Codelist Name": "name",
    "Term Signification": "value",
    "Synonyms": "synonym",
    "Definition": "definition",
    "Term": "term",
    "Standard and Date": "standard_and_date",
}


class SqlGetCodelistAttributesOperation(SqlBaseOperation):
    """
    Retrieves a list of codelist attributes (e.g. Term CCODEs) for a specific
    standard version defined in a dataset column.
    """

    def _execute_operation(self):
        ct_table = StaticTables.IG_CODELIST_TABLE_NAME.value
        attribute = self.params.ct_attribute

        raw_col = self.data_service.pgi.schema.get_column_hash(ct_table, _COLUMN_MAP.get(attribute, "item_code"))

        if attribute == "Synonym":
            select_col_sql = f"TRIM(UNNEST(STRING_TO_ARRAY({raw_col}, ';')))"
        else:
            select_col_sql = raw_col

        version_date_col_sql = self.data_service.pgi.schema.get_column_hash(ct_table, "version_date")
        std_type_col_sql = self.data_service.pgi.schema.get_column_hash(ct_table, "standard_type")

        where_clauses = []
        version_clause, params = self._ct_version_filter(std_type_col_sql, version_date_col_sql)
        if version_clause:
            where_clauses.append(version_clause)

        conditions = self.params.ct_conditions
        if conditions:
            for condition in conditions:
                for k, v in condition.items():
                    column_sql = self.data_service.pgi.schema.get_column_hash(ct_table, self._condition_column(k))
                    if v is None:
                        where_clauses.append(f"{column_sql} IS NULL")
                    else:
                        escaped_value = str(v).replace("'", "''")
                        where_clauses.append(f"{column_sql} = '{escaped_value}'")

        base_query = f"""
            SELECT DISTINCT {select_col_sql} AS value
            FROM {ct_table}
        """

        if where_clauses:
            query = f"{base_query} WHERE {' AND '.join(where_clauses)}"
        else:
            query = base_query

        return SqlOperationResult(query=query, type="collection", subtype="Char", params=params or None)

    @staticmethod
    def _condition_column(attribute: str) -> str:
        """
        Maps a ct_conditions attribute (e.g. "Codelist Code") to its column. The CLI's rule loader
        replaces spaces in rule keys with underscores, so "Codelist_Code" is accepted too.
        """
        column = _COLUMN_MAP.get(attribute) or _COLUMN_MAP.get(str(attribute).replace("_", " "))
        if not column:
            raise ValueError(
                f"Unknown ct_conditions attribute '{attribute}'. Expected one of: {', '.join(_COLUMN_MAP)}"
            )
        return column

    def _ct_version_filter(self, std_type_col: str, version_date_col: str) -> Tuple[Optional[str], dict]:
        """
        Builds the WHERE clause restricting codelist rows to the CT version(s) in use, plus any
        query params it needs. CT version precedence:
        - the version in an operation-referenced column (e.g. TSVCDVER), where populated with a loaded CT package
        - the provided codelists (Library sheet / -ct), or else a literal version given in the operation
        - otherwise all loaded CT packages
        """
        ct_version_column = self._ct_version_column()
        fallback_versions = self.data_service.provided_codelists or (
            None if ct_version_column else self.params.ct_version
        )
        version_clause = self._version_clause(ct_version_column, fallback_versions, std_type_col, version_date_col)
        params = {"$ct_version": ct_version_column} if ct_version_column else {}
        return version_clause, params

    def _ct_version_column(self) -> Optional[str]:
        ct_version = self.params.ct_version
        if not ct_version or not isinstance(ct_version, str) or not self.params.table:
            return None
        if not self.data_service.pgi.schema.column_exists(self.params.table, ct_version):
            return None
        return ct_version

    def _version_clause(
        self,
        ct_version_column: Optional[str],
        fallback_versions,
        std_type_col: str,
        version_date_col: str,
    ) -> Optional[str]:
        fallback_clause = None
        if fallback_versions:
            ct_list = sorted(fallback_versions) if isinstance(fallback_versions, (list, set)) else [fallback_versions]
            fallback_clause = self._build_clauses(self._parse_versions(ct_list), std_type_col, version_date_col)
        if not ct_version_column:
            return fallback_clause
        record_version = "TRIM(CAST($ct_version AS TEXT))"
        record_clause = f"{version_date_col} = {record_version}"
        if not fallback_clause:
            return record_clause
        # empty record versions, or naming packages missing from the cache, fall back to the provided codelists
        ct_table = StaticTables.IG_CODELIST_TABLE_NAME.value
        loaded_col = self.data_service.pgi.schema.get_column_hash(ct_table, "version_date")
        self.data_service.pgi.execute_sql(
            f"SELECT DISTINCT {loaded_col} AS version FROM {ct_table} WHERE {loaded_col} IS NOT NULL"
        )
        loaded_versions = sorted(row["version"] for row in self.data_service.pgi.fetch_all())
        if not loaded_versions:
            return fallback_clause
        known_version = f"{record_version} IN ({', '.join(repr(version) for version in loaded_versions)})"
        return f"(CASE WHEN {known_version} THEN {record_clause} ELSE {fallback_clause} END)"

    def _parse_versions(self, ct_list: list) -> list:
        provided_cts = []
        for ct in ct_list:
            if isinstance(ct, str):
                if "ct-" in ct:
                    parts = ct.split("ct-")
                    provided_cts.append({"type": parts[0], "version": parts[1]})
                else:
                    provided_cts.append({"type": None, "version": ct})
        return provided_cts

    def _build_clauses(self, provided_cts: list, std_type_col: str, version_date_col: str) -> str:
        or_conditions = []
        for ct in provided_cts:
            and_conditions = []
            if ct["type"]:
                and_conditions.append(f"{std_type_col} = '{ct['type']}'")
            if ct["version"]:
                and_conditions.append(f"{version_date_col} = '{ct['version']}'")
            if and_conditions:
                or_conditions.append(f"({' AND '.join(and_conditions)})")

        return f"({' OR '.join(or_conditions)})" if or_conditions else "TRUE"
