from cdisc_rules_engine.data_service.merges.join import SqlJoinMerge
from cdisc_rules_engine.models.sql_operation_result import SqlOperationResult
from cdisc_rules_engine.sql_operations.sql_base_operation import SqlBaseOperation


class SqlDayDataValidatorOperation(SqlBaseOperation):
    def _execute_operation(self):
        """
        Calculate Study Day (--DY) values by computing the difference between
        a date-time column (--DTC) and the reference start date (RFSTDTC) from the DM dataset.

        CDISC Algorithm:
        - If --DTC >= RFSTDTC: --DY = (--DTC date) - (RFSTDTC date) + 1
        - If --DTC < RFSTDTC: --DY = (--DTC date) - (RFSTDTC date)
        - No Study Day 0 exists (goes from -1 to +1)
        """
        current_table = self.data_service.pgi.schema.get_table(self.params.domain)
        if not current_table:
            raise ValueError(f"Table for domain {self.params.domain} not found")

        dm_table = self.data_service.pgi.schema.get_table("DM")
        if not dm_table:
            # Return 0 for all rows if DM doesn't exist
            id_col = self.data_service.pgi.schema.get_column_hash(self.params.domain, "id")
            query = f"SELECT {id_col} AS id, 0 AS value FROM {current_table.hash} ORDER BY id"
            return SqlOperationResult(query=query, type="table", subtype="Num")

        joined_table = SqlJoinMerge.perform_join(
            pgi=self.data_service.pgi,
            left=current_table,
            right=dm_table,
            pivot_left=["USUBJID"],
            pivot_right=["USUBJID"],
            type="LEFT",
        )

        # This handles date parsing and validation using COMPLETE_DATE_REGEX
        target_date_col = self.data_service.pgi.generate_date_column(joined_table.name, self.params.target)

        if not joined_table.has_column("RFSTDTC"):
            raise ValueError("RFSTDTC column not found in joined table")

        rfstdtc_date_col = self.data_service.pgi.generate_date_column(joined_table.name, "RFSTDTC")

        # Build the DY calculation query using the generated date columns
        id_col = self.data_service.pgi.schema.get_column_hash(self.params.domain, "id")
        query = f"""
        SELECT
            {id_col} AS id,
            CASE
                WHEN {target_date_col.hash} IS NULL OR {rfstdtc_date_col.hash} IS NULL THEN NULL
                WHEN DATE({target_date_col.hash}) >= DATE({rfstdtc_date_col.hash}) THEN
                    (DATE({target_date_col.hash}) - DATE({rfstdtc_date_col.hash})) + 1
                ELSE
                    DATE({target_date_col.hash}) - DATE({rfstdtc_date_col.hash})
            END AS value
        FROM {joined_table.hash}
        """

        return SqlOperationResult(query=query, type="table", subtype="Num")
