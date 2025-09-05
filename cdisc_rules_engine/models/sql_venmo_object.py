from business_rules.variables import BaseVariables, rule_variable

from cdisc_rules_engine.check_operators.sql import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)


def sql_rule_variable(label=None, options=None):
    return rule_variable(PostgresQLOperators, label=label, options=options)


class SqlVenmoObject(BaseVariables):
    """
    This class is passed to venmo and is used to translate check operator
    names to their implementations.
    """

    def __init__(self, dataset_id: str, data_service: PostgresQLDataService, **params):
        self.dataset_id = dataset_id
        self.data_service = data_service
        self.params = params

    # common variables
    @sql_rule_variable()
    def get_dataset(self) -> dict:
        return {
            "dataset_id": self.dataset_id,
            "data_service": self.data_service,
            **self.params,
        }
