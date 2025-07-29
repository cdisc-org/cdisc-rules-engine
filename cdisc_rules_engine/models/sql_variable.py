from business_rules.variables import BaseVariables, rule_variable
from pandas import DataFrame
from cdisc_rules_engine.check_operators.sql_operators import SQLDataframeType


def sql_rule_variable(label=None, options=None):
    return rule_variable(SQLDataframeType, label=label, options=options)


class SQLVariable(BaseVariables):
    """
    The class represents a dataset variable which
    holds a pandas DataFrame as a dataset.
    The engine uses operators like equal_to, matches_regex etc.
    to validate the dataset columns.
    """

    def __init__(self, dataset: DataFrame, **params):
        self.dataset = dataset
        self.params = params

    # common variables
    @sql_rule_variable(label="GET DATASET")
    def get_dataset(self) -> dict:
        return {"value": self.dataset, **self.params}
