from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation

_COLUMN_MAP = {
    "Term CCODE": "item_code",
    "Term Signification": "value",
    "Codelist Code": "codelist_code",
    "Codelist Name": "name",
    "Term": "term",
}


class SqlGetCodelistAttributesOperation(SqlBaseOperation):
    """
    Retrieves a list of codelist attributes (e.g. Term CCODEs) for a specific
    standard version defined in a dataset column.
    """

    def _execute_operation(self):
        ct_table = StaticTables.IG_CODELIST_TABLE_NAME.value
        attribute = self.params.ct_attribute
        if not self.params.ct_version and not self.data_service.provided_codelists:
            raise ValueError("Version must be provided for codelist attribute retrieval.")
        ct_list = (
            self.data_service.provided_codelists
            if isinstance(self.data_service.provided_codelists, list)
            else [self.data_service.provided_codelists]
        )
        provided_cts = [{"type": ct.split("ct-")[0], "version": ct.split("ct-")[1]} for ct in ct_list]
        conditions = self.params.ct_conditions

        select_col_sql = self.data_service.pgi.schema.get_column_hash(ct_table, _COLUMN_MAP.get(attribute, "item_code"))
        version_date_col_sql = self.data_service.pgi.schema.get_column_hash(ct_table, "version_date")
        std_type_col_sql = self.data_service.pgi.schema.get_column_hash(ct_table, "standard_type")

        where_clause = f"""({" OR ".join(f"({std_type_col_sql} = '{ct['type']}' AND {version_date_col_sql} = '{ct['version']}')" for ct in provided_cts)})"""  # noqa

        query = f"""
            SELECT DISTINCT {select_col_sql} AS value
            FROM {ct_table}
            WHERE {where_clause}
        """

        condition_sql = """"""
        if conditions:
            for condition in conditions:
                for k, v in condition.items():
                    _COLUMN_MAP.get(k)
                    condition_sql += f""" AND {_COLUMN_MAP.get(k)} = '{v}'"""

        if condition_sql:
            query += condition_sql

        return SqlOperationResult(query=query, type="collection", subtype="Char")
