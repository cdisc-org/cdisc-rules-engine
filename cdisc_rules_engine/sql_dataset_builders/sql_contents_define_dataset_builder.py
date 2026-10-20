from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
    DEFINE_DATASETS_TYPE,
)


class SqlContentsDefineDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Dataset Contents Check against Define XML rules.
    Adds dataset metadata from Define XML to the dataset for use in rules.
    """

    def build(self) -> str:  # noqa
        table_id = self.data_service.get_dataset_for_rule(self.dataset_metadata, self.rule, self.standards_context)

        schema = self.data_service.pgi.schema.get_table(table_id)
        if not schema:
            raise ValueError(f"Table {table_id} not found")

        for col in ["dataset_location", "dataset_name", "dataset_label", "dataset_domain"]:
            if not self.data_service.pgi.schema.get_column(table_id, col):
                self.data_service.pgi.add_column(table_id, SqlColumnSchema.define(col, "Char"))

        define_ds_metadata = self.get_define_dataset()
        for col, type in DEFINE_DATASETS_TYPE.items():
            if not self.data_service.pgi.schema.get_column(table_id, col):
                self.data_service.pgi.add_column(table_id, SqlColumnSchema.define(col, type))

        dataset_location = self.dataset_metadata.filename
        dataset_name = self.dataset_metadata.name
        dataset_label = self.dataset_metadata.label
        dataset_domain = self.dataset_metadata.domain

        table_hash = self.data_service.pgi.schema.get_table_hash(table_id)

        row = {
            "dataset_location": dataset_location,
            "dataset_name": dataset_name,
            "dataset_label": dataset_label,
            "dataset_domain": dataset_domain,
        }
        row.update(define_ds_metadata)

        set_values = []
        for col, value in row.items():
            col_hash = self.data_service.pgi.schema.get_column_hash(table_id, col)
            if value is None:
                set_values.append(f"{col_hash} = NULL")
            elif isinstance(value, str):
                escaped = value.replace("'", "''")
                set_values.append(f"{col_hash} = '{escaped}'")
            else:
                set_values.append(f"{col_hash} = {value}")
        set_query = ", ".join(set_values)

        update_query = f"UPDATE {table_hash} SET {set_query};"
        self.data_service.pgi.execute_sql(update_query)

        if not self.data_service.pgi.schema.get_column(table_id, "define_key_sequence_is_unique"):
            self.data_service.pgi.add_column(table_id, SqlColumnSchema.define("define_key_sequence_is_unique", "Bool"))
        key_sequence = [col for col in (define_ds_metadata.get("define_dataset_key_sequence") or "").split(",") if col]

        # handle SUPP datasets - if SUPP exists then it may contribute key variables, and so join is necessary.
        supp_table_id = f"SUPP{table_id}".lower()
        supp_table_hash = self.data_service.pgi.schema.get_table_hash(supp_table_id)
        if supp_table_hash:
            idvar_cols_query = f"""SELECT STRING_AGG(DISTINCT IDVAR, ', ') as idvars, STRING_AGG(DISTINCT QNAM, ', ') as qnams from {supp_table_hash};"""  # noqa
            self.data_service.pgi.execute_sql(idvar_cols_query)
            all_cols = self.data_service.pgi.fetch_one()
            # a blank IDVAR (e.g. in SUPPDM) links the SUPP record to the subject rather than to a record
            idvar_cols = (all_cols["idvars"] or "").split(", ")
            qnam_cols = (all_cols["qnams"] or "").split(", ")
            non_supp_key_cols = [col for col in key_sequence if col not in qnam_cols]
            key_vars = ", ".join(non_supp_key_cols + ["final_supp"]) if key_sequence else ""

            pivoted_supp_query = f"""
                WITH pivoted_supp as (
                    SELECT
                        STUDYID, RDOMAIN, USUBJID, IDVARVAL, IDVAR,
                        jsonb_object_agg(QNAM, QVAL) AS all_supp_vars
                    FROM {supp_table_hash}
                    GROUP BY 1, 2, 3, 4, 5
                )"""

            from_query = f"""
                (SELECT
                    {', '.join(['a.id'] + [f'a.{col}' for col in non_supp_key_cols])}
                    ,COALESCE({', '.join([f'ps_{i}.all_supp_vars' for i in range(len(idvar_cols))])}) as final_supp
                FROM {table_hash} a
                {' \n'.join([self._supp_join(i, col) for i, col in enumerate(idvar_cols)])}
                ) sub_inner"""
        else:
            key_vars = ", ".join(key_sequence)
            from_query = table_hash
            pivoted_supp_query = ""

        unique_col_hash = self.data_service.pgi.schema.get_column_hash(table_id, "define_key_sequence_is_unique")
        uniqueness_check = f"(COUNT(*) OVER (PARTITION BY {key_vars}) = 1)" if key_vars else "true"
        uniqueness_query = f"""
            {pivoted_supp_query}
            UPDATE {table_hash} t
            SET {unique_col_hash} = sub.unique_status
            FROM (
                SELECT id,
                {uniqueness_check} as unique_status
                FROM {from_query}
            ) sub
            WHERE t.id = sub.id;
        """
        self.data_service.pgi.execute_sql(uniqueness_query)

        return table_id

    @staticmethod
    def _supp_join(index: int, idvar: str) -> str:
        join = f"""LEFT JOIN pivoted_supp ps_{index}
            ON a.STUDYID = ps_{index}.STUDYID
            AND a.DOMAIN = ps_{index}.RDOMAIN
            AND a.USUBJID = ps_{index}.USUBJID"""
        if not idvar:
            return f"{join}\n            AND COALESCE(ps_{index}.IDVAR, '') = ''"
        return f"""{join}
            AND ps_{index}.IDVAR = '{idvar}'
            AND a.{idvar}::text = ps_{index}.IDVARVAL::text"""
