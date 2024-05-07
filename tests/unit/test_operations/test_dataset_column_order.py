from unittest.mock import MagicMock

import pandas as pd
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset

from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.dataset_column_order import DatasetColumnOrder
import pytest


@pytest.mark.parametrize("dataset_type", [(PandasDataset), (DaskDataset)])
def test_get_column_order_from_dataset(operation_params: OperationParams, dataset_type):
    """
    Unit test for DataProcessor.get_column_order_from_dataset.
    """
    operation_params.dataframe = dataset_type.from_dict(
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
