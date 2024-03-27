from unittest.mock import MagicMock

import pandas as pd
from cdisc_rules_engine.models.dataset.dask_dataset import DaskDataset
from cdisc_rules_engine.models.dataset.pandas_dataset import PandasDataset

from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.operations.record_count import RecordCount
import pytest


@pytest.mark.parametrize("dataset_type", [(PandasDataset), (DaskDataset)])
def test_record_count_operation(operation_params: OperationParams, dataset_type):
    """
    Unit test for RecordCount operation.
    Creates a dataframe and checks that
    the operation returns correct number of records.
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
    operation = RecordCount(
        operation_params, operation_params.dataframe, MagicMock(), MagicMock()
    )
    result: pd.DataFrame = operation.execute()
    expected: pd.Series = pd.Series(
        [
            2,
            2,
        ]
    )
    assert result[operation_params.operation_id].equals(expected)
