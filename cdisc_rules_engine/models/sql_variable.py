from business_rules.variables import BaseVariables, rule_variable
from pandas import DataFrame
from cdisc_rules_engine.check_operators.sql_operators import SQLDataframeType
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService


def sql_rule_variable(label=None, options=None):
    return rule_variable(SQLDataframeType, label=label, options=options)


class SQLVariable(BaseVariables):
    """
    The class represents a dataset variable which
    holds a pandas DataFrame as a dataset.
    The engine uses operators like equal_to, matches_regex etc.
    to validate the dataset columns.
    """

    def __init__(
        self, validation_dataset_id: str, sql_data_service: PostgresQLDataService, dataset: DataFrame, **params
    ):
        self.validation_dataset_id = validation_dataset_id
        self.sql_data_service = sql_data_service
        self.dataset = dataset
        self.params = params

    # common variables
    @sql_rule_variable(label="GET DATASET")
    def get_dataset(self) -> dict:
        return {
            "df": self.dataset,
            "validation_dataset_id": self.validation_dataset_id,
            "sql_data_service": self.sql_data_service,
            **self.params,
        }

    # TODO: fix when results is serialized into a proper python object
    # (https://docs.google.com/document/d/151PkKjumpIBOysETQd9zXkAzvj1fLmvuRkkD0KyAhsg/edit?tab=t.0)
    def get_error_rows(self, results) -> DataFrame:
        data_with_results = self.dataset.copy()
        data_with_results["results"] = results
        return data_with_results[data_with_results["results"].isin([True])]

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
