from unittest.mock import MagicMock
import pandas as pd
import dask.dataframe as dd
import pytest
from cdisc_rules_engine.models.operation_params import OperationParams
from cdisc_rules_engine.DatasetOperations.Operations import DatasetOperations


@pytest.mark.parametrize(
    "dataframe, expected",
    [
        (
            pd.DataFrame.from_dict(
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
            ),
            pd.Series(
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
                ],
                dtype=object,  # Set the dtype to 'object'
            ),
        ),
        (
            dd.from_pandas(
                pd.DataFrame.from_dict(
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
                ),
                npartitions=1,
            ),
            pd.Series(
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
                ],
                dtype=object,  # Set the dtype to 'object'
            ),
        ),
    ],
)
def test_get_column_order_from_dataset(
    operation_params: OperationParams, dataframe, expected
):
    """
    Unit test for DataProcessor.get_column_order_from_dataset.
    """
    operation_params.dataframe = dataframe
    operations = DatasetOperations()
    result = operations.get_service(
        "get_column_order_from_dataset",
        operation_params,
        operation_params.dataframe,
        MagicMock(),
        MagicMock(),
    )
    result_series = result[operation_params.operation_id]
    # assert result_series.equals(expected)
    for res, exp in zip(result_series, expected):
        assert res == exp
