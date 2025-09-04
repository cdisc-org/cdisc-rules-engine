from business_rules.variables import BaseVariables, rule_variable
from pandas import DataFrame

from cdisc_rules_engine.check_operators.sql import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)


def sql_rule_variable(label=None, options=None):
    return rule_variable(PostgresQLOperators, label=label, options=options)


class PostgresQLBusinessEngineObject(BaseVariables):
    """
    The class represents a dataset variable which
    holds a pandas DataFrame as a dataset.
    The engine uses operators like equal_to, matches_regex etc.
    to validate the dataset columns.
    """

    def __init__(self, validation_dataset_id: str, sql_data_service: PostgresQLDataService, **params):
        self.validation_dataset_id = validation_dataset_id
        self.sql_data_service = sql_data_service
        self.params = params

    # common variables
    @sql_rule_variable(label="GET DATASET")
    def get_dataset(self) -> dict:
        return {
            "validation_dataset_id": self.validation_dataset_id,
            "sql_data_service": self.sql_data_service,
            **self.params,
        }

    # TODO: Clean this up properly
    def get_error_rows(self, truth_series) -> DataFrame:
        true_indicies = [str(i + 1) for i, x in enumerate(truth_series) if x]
        self.sql_data_service.pgi.execute_sql(
            f"""SELECT * FROM
                {self.sql_data_service.pgi.schema.get_table_hash(self.validation_dataset_id)}
            WHERE id IN ({', '.join(true_indicies)})"""
        )
        results = self.sql_data_service.pgi.fetch_all()
        return DataFrame(results)

    def get_columns(self) -> list[str]:
        query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{self.validation_dataset_id.lower()}'
        AND table_schema = 'public'
        ORDER BY ordinal_position;
        """
        self.sql_data_service.pgi.execute_sql(query=query)
        results = self.sql_data_service.pgi.fetch_all()
        return [res["column_name"] for res in results]
