from unittest.mock import MagicMock

import pandas as pd

from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.dataset_column_order import DatasetColumnOrder


def test_get_column_order_from_dataset(operation_params: OperationParams):
    """
    Unit test for DataProcessor.get_column_order_from_dataset.
    """
    operation_params.dataframe = pd.DataFrame.from_dict(
        {
            "STUDYID": [
                "CDISC01",
                "CDISC01",
            ],
            "DOMAIN": [
                "AE",
                "AE",
            ],
            "AESEQ": [
                1,
                2,
            ],
            "USUBJID": [
                "TEST1",
                "TEST1",
            ],
        }
    )
    operation = DatasetColumnOrder(
        operation_params, operation_params.dataframe, MagicMock(), MagicMock()
    )
    result: pd.DataFrame = operation.execute()
    expected: pd.Series = pd.Series(
        [
            [
                "STUDYID",
                "DOMAIN",
                "AESEQ",
                "USUBJID",
            ],
            [
                "STUDYID",
                "DOMAIN",
                "AESEQ",
                "USUBJID",
            ],
        ]
    )
    assert result[operation_params.operation_id].equals(expected)
