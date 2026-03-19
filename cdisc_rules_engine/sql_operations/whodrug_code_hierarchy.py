from cdisc_rules_engine.enums.static_tables import StaticTables
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlWhodrugHierarchyOperation(SqlBaseOperation):
    def _execute_operation(self):
        domain = self.params.domain
        dataset_id = self.data_service.pgi.schema.get_table_hash(domain)

        decod_col = self.data_service.pgi.schema.get_column_hash(domain, f"{domain}DECOD")
        clas_col = self.data_service.pgi.schema.get_column_hash(domain, f"{domain}CLAS")
        clascd_col = self.data_service.pgi.schema.get_column_hash(domain, f"{domain}CLASCD")
        if any(col is None for col in [decod_col, clas_col, clascd_col]):
            raise Exception(f"One or more required columns are missing: {decod_col}, {clas_col}, {clascd_col}")

        query = f"""
            SELECT
                CASE
                    WHEN {clas_col} = 'MULTIPLE' OR {clascd_col} = 'MULTIPLE' THEN
                        CASE
                            WHEN dc.instance_count > 1 THEN TRUE
                            ELSE FALSE
                        END
                    ELSE
                        CASE
                            WHEN em.drug_name IS NOT NULL THEN TRUE
                            ELSE FALSE
                        END
                END AS value
            FROM {dataset_id}
            LEFT JOIN (
                SELECT
                    drug_name,
                    COUNT(*) AS instance_count
                FROM {StaticTables.WHODRUG_TABLE_NAME.value}
                GROUP BY drug_name
            ) dc
                ON {decod_col} = dc.drug_name
            LEFT JOIN (
                SELECT DISTINCT
                    drug_name,
                    level_4,
                    atc_code
                FROM {StaticTables.WHODRUG_TABLE_NAME.value}
            ) em
                ON {decod_col} = em.drug_name
                AND {clas_col} = em.level_4
                AND {clascd_col} = em.atc_code
        """

        return SqlOperationResult(query=query, type="collection", subtype="Bool")
