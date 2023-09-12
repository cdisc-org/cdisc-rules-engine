from business_rules.variables import BaseVariables, rule_variable
from cdisc_rules_engine.rule_operators.dataframe_operators import DataframeType
from pandas import DataFrame


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
    @rule_variable(DataframeType, label="GET DATASET")
    def get_dataset(self) -> dict:
        return {"value": self.dataset, **self.params}
