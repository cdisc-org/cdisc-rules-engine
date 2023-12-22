from unittest.mock import MagicMock

import pandas as pd
import dask.dataframe as dd

from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.DatasetOperations.Operations import DatasetOperations


def test_record_count_operation(operation_params: OperationParams):
    """
    Unit test for RecordCount operation.
    Creates a dataframe and checks that
    the operation returns correct number of records.
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
    operations = DatasetOperations()
    result = operations.get_service(
        "record_count",
        operation_params,
        operation_params.dataframe,
        MagicMock(),
        MagicMock(),
    )
    expected: pd.Series = pd.Series(
        [
            2,
            2,
        ]
    )
    assert result[operation_params.operation_id].equals(expected)


def test_record_count_operation_dask(operation_params: OperationParams):
    """
    Unit test for RecordCount operation.
    Creates a dataframe and checks that
    the operation returns correct number of records.
    """
    operation_params.dataframe = dd.DataFrame.from_dict(
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
        },
        npartitions=1,
    )
    operations = DatasetOperations()
    result = operations.get_service(
        "record_count",
        operation_params,
        operation_params.dataframe,
        MagicMock(),
        MagicMock(),
    )
    expected: pd.Series = pd.Series(
        [
            2,
            2,
        ]
    )
    assert result[operation_params.operation_id].equals(expected)
