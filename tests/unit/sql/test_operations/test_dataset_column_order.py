from .helpers import (
    assert_operation_collection,
    setup_sql_operations,
)
import pytest


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            {
                "STUDYID": ["CDISC01", "CDISC01"],
                "DOMAIN": ["AE", "AE"],
                "AESEQ": [1, 2],
                "USUBJID": ["TEST1", "TEST1"],
            },
            ["STUDYID", "DOMAIN", "AESEQ", "USUBJID"],
        ),
        (
            {
                "DOMAIN": ["DM", "DM"],
                "USUBJID": ["SUBJ1", "SUBJ2"],
                "SUBJID": ["ID1", "ID2"],
            },
            ["DOMAIN", "USUBJID", "SUBJID"],
        ),
    ],
)
def test_sql_dataset_column_order(data, expected):
    operation = setup_sql_operations("get_column_order_from_dataset", None, data)
    result = operation.execute()
    assert_operation_collection(operation, result, expected)
