from .helpers import (
    assert_operation_collection,
    setup_sql_operations,
)
import pytest
from unittest.mock import patch

test_data = {
    "STUDYID": ["TEST_STUDY", "TEST_STUDY", "TEST_STUDY"],
    "AETERM": ["test", "test", "test"],
}

# have included ordinals, although currently the ordering is done within the function I'm mocking, so they are redundant
# may need to think about adding a test for ordering? not sure if necessary
mock_metadata = [
    {
        "name": "STUDYID",
        "ordinal": 1,
    },
    {
        "name": "DOMAIN",
        "ordinal": 2,
    },
    {
        "name": "USUBJID",
        "ordinal": 3,
    },
    {
        "name": "AESTDTC",
        "ordinal": 4,
    },
    {
        "name": "AETERM",
        "ordinal": 17,
    },
    {
        "name": "AESCAN",
        "ordinal": 18,
    },
    {
        "name": "AESCONG",
        "ordinal": 33,
    },
]


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            test_data,
            ["STUDYID", "DOMAIN", "USUBJID", "AESTDTC", "AETERM", "AESCAN", "AESCONG"],
        ),
    ],
)
def test_get_model_column_order(data, expected):
    operation = setup_sql_operations("get_model_column_order", None, data, standards_context="sdtm")

    with patch.object(operation, "_get_variables_metadata_from_standard_model", return_value=mock_metadata):
        result = operation.execute()
        assert_operation_collection(operation, result, expected)
