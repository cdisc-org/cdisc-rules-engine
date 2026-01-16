from typing import List

from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema
from cdisc_rules_engine.models.sql.table_schema import SqlTableSchema
from cdisc_rules_engine.sql_dataset_builders.sql_base_dataset_builder import (
    SqlBaseDatasetBuilder,
)


class SqlDomainListDatasetBuilder(SqlBaseDatasetBuilder):
    """
    Builder for Domain Presence Check rules.
    Creates a physical table with one row containing all available domains as columns.

    Example table structure:
       AE      EC           DM
    -------|--------|----------------
    ae.xpt | ec.xpt | dm1.xpt,dm2.xpt
    """

    def build(self) -> str:
        """
        Create (or replace) a domains catalog table and return the table name.
        The table has one row with each domain as a column containing its filename.
        """
        table_name = "domains_catalog_table"

        if self.data_service.pgi.schema.get_table(table_name) is not None:
            return table_name

        domain_dict: dict[str, List[str]] = {}
        for ds in self.datasets:
            if not ds.domain:  # Only include datasets with a domain
                continue

            if ds.domain not in domain_dict:
                domain_dict[ds.domain] = []
            domain_dict[ds.domain].append(ds.filename)

        schema = SqlTableSchema.derived(table_name, self.data_service.pgi)

        if domain_dict:
            for domain in domain_dict.keys():
                schema.add_column(SqlColumnSchema.generated(domain, "Char"))
        else:
            schema.add_column(SqlColumnSchema.generated("_placeholder", "Num"))

        self.data_service.pgi.create_table(schema)

        if domain_dict:
            columns = [schema.get_column_hash(domain) for domain in domain_dict.keys()]
            values = [f"'{",".join(filenames)}'" for filenames in domain_dict.values()]

            insert_sql = f"""
                INSERT INTO {schema.hash} ({', '.join(columns)})
                VALUES ({', '.join(values)});
            """
            self.data_service.pgi.execute_sql(insert_sql)

        return table_name
