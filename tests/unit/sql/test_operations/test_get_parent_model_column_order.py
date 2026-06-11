from unittest.mock import patch
import pytest

from .helpers import assert_operation_collection, setup_sql_operations


test_data = {
    "STUDYID": ["TEST_STUDY", "TEST_STUDY", "TEST_STUDY"],
    "AETERM": ["test", "test", "test"],
}


def side_effect_func(rdomain):
    if rdomain == "AE":
        return [
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

    return []


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            test_data,
            ["STUDYID", "DOMAIN", "USUBJID", "AESTDTC", "AETERM", "AESCAN", "AESCONG"],
        ),
    ],
)
def test_get_parent_model_column_order(data, expected):
    operation = setup_sql_operations("get_parent_model_column_order", None, data, standards_context="sdtm")
    with patch.object(operation, "_get_rdomain", return_value="AE"):
        with patch.object(operation, "_get_variables_metadata_from_standard_model", side_effect=side_effect_func):
            result = operation.execute()
            assert_operation_collection(operation, result, expected)
