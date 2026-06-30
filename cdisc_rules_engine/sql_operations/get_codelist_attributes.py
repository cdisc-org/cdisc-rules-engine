from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation

_COLUMN_MAP = {
    "Term CCODE": "item_code",
    "Term Signification": "value",
    "Codelist Code": "codelist_code",
    "Codelist Name": "name",
    "Term": "term",
    "Synonyms": "synonym",
    "Definition": "definition",
    "Extensible": "extensible",
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

        raw_versions = self.data_service.provided_codelists or self.params.ct_version
        if raw_versions:
            ct_list = raw_versions if isinstance(raw_versions, list) else [raw_versions]
            provided_cts = self._parse_versions(ct_list)
            where_clause = self._build_clauses(provided_cts, std_type_col_sql, version_date_col_sql)
            where_clauses.append(where_clause)

        conditions = self.params.ct_conditions
        if conditions:
            for condition in conditions:
                for k, v in condition.items():
                    where_clauses.append(f"{_COLUMN_MAP.get(k)} = '{v}'")

        base_query = f"""
            SELECT DISTINCT {select_col_sql} AS value
            FROM {ct_table}
        """

        if where_clauses:
            query = f"{base_query} WHERE {' AND '.join(where_clauses)}"
        else:
            query = base_query

        return SqlOperationResult(query=query, type="collection", subtype="Char")

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
