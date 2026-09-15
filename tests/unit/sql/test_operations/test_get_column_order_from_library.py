from unittest.mock import patch

import pytest

from .helpers import (
    assert_operation_collection,
    setup_sql_operations,
)

test_data = {
    "STUDYID": ["TEST_STUDY", "TEST_STUDY", "TEST_STUDY"],
    "AETERM": ["test", "test", "test"],
}

mock_metadata = [
    {
        "name": "STUDYID",
        "role": "Identifier",
        "ordinal": 1,
    },
    {
        "name": "DOMAIN",
        "role": "Identifier",
        "ordinal": 2,
    },
    {
        "name": "USUBJID",
        "role": "Identifier",
        "ordinal": 3,
    },
    {
        "name": "--TERM",
        "role": "Topic",
        "ordinal": 4,
    },
    {
        "name": "VISITNUM",
        "role": "Timing",
        "ordinal": 17,
    },
    {
        "name": "VISIT",
        "role": "Timing",
        "ordinal": 18,
    },
]


def test_get_column_order_from_library():
    """The full ordinal-sorted variable list is returned, with -- replaced by the domain."""
    operation = setup_sql_operations("get_column_order_from_library", None, test_data, standards_context="sdtm")

    with patch.object(operation, "_get_variables_metadata_from_standard", return_value=mock_metadata):
        result = operation.execute()
        assert_operation_collection(
            operation,
            result,
            ["STUDYID", "DOMAIN", "USUBJID", "test_tableTERM", "VISITNUM", "VISIT"],
        )


@pytest.mark.parametrize(
    "key_name, key_value, expected",
    [
        ("role", "Timing", ["VISITNUM", "VISIT"]),
        ("role", "Identifier", ["STUDYID", "DOMAIN", "USUBJID"]),
        ("role", "NonExistentRole", []),
        ("role", "", ["STUDYID", "DOMAIN", "USUBJID", "test_tableTERM", "VISITNUM", "VISIT"]),
        (None, None, ["STUDYID", "DOMAIN", "USUBJID", "test_tableTERM", "VISITNUM", "VISIT"]),
    ],
)
def test_get_column_order_from_library_with_filter(key_name, key_value, expected):
    """key_name/key_value optionally filter the variables before names are extracted."""
    operation = setup_sql_operations(
        "get_column_order_from_library",
        None,
        test_data,
        standards_context="sdtm",
        extra_config={"key_name": key_name, "key_value": key_value},
    )

    with patch.object(operation, "_get_variables_metadata_from_standard", return_value=mock_metadata):
        result = operation.execute()
        assert_operation_collection(operation, result, expected)


def test_get_column_order_from_library_deduplicates_preserving_order():
    """Duplicate variable names (e.g. present in both model and IG metadata) are deduplicated."""
    duplicated_metadata = [
        {"name": "STUDYID", "ordinal": 1},
        {"name": "DOMAIN", "ordinal": 2},
        {"name": "STUDYID", "ordinal": 3},
    ]
    operation = setup_sql_operations("get_column_order_from_library", None, test_data, standards_context="sdtm")

    with patch.object(operation, "_get_variables_metadata_from_standard", return_value=duplicated_metadata):
        result = operation.execute()
        assert_operation_collection(operation, result, ["STUDYID", "DOMAIN"])


def test_get_column_order_from_library_exception_handling():
    """Metadata retrieval failures should raise, since the rule can't run without it."""
    operation = setup_sql_operations("get_column_order_from_library", None, test_data, standards_context="sdtm")

    with patch.object(
        operation, "_get_variables_metadata_from_standard", side_effect=Exception("Metadata retrieval failed")
    ):
        with pytest.raises(Exception, match="Metadata retrieval failed"):
            operation.execute()
