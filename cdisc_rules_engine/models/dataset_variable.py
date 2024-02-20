from business_rules.variables import BaseVariables, rule_variable
from pandas import DataFrame
from cdisc_rules_engine.check_operators.dataframe_operators import DataframeType


def dataframe_rule_variable(label=None, options=None):
    return rule_variable(DataframeType, label=label, options=options)


class DatasetVariable(BaseVariables):
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
    @dataframe_rule_variable(label="GET DATASET")
    def get_dataset(self) -> dict:
        return {"value": self.dataset, **self.params}
