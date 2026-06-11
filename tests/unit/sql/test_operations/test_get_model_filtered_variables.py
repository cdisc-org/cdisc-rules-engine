from unittest.mock import patch

import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.enums.variable_roles import VariableRoles
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)
from cdisc_rules_engine.standards.default_standards_context import (
    DefaultStandardsContext,
)

from .helpers import (
    assert_operation_collection,
)

# Test data sets matching the original operation tests
test_set1_variables = [
    {
        "name": "STUDYID",
        "role": VariableRoles.IDENTIFIER.value,
        "ordinal": 1,
    },
    {
        "name": "DOMAIN",
        "role": VariableRoles.IDENTIFIER.value,
        "ordinal": 2,
    },
    {
        "name": "USUBJID",
        "role": VariableRoles.IDENTIFIER.value,
        "ordinal": 3,
    },
    {
        "name": "AETERM",
        "role": VariableRoles.IDENTIFIER.value,
        "ordinal": 4,
    },
    {
        "name": "VISITNUM",
        "role": VariableRoles.TIMING.value,
        "ordinal": 17,
    },
    {
        "name": "VISIT",
        "role": VariableRoles.TIMING.value,
        "ordinal": 18,
    },
    {
        "name": "TIMING_VAR",
        "role": VariableRoles.TIMING.value,
        "ordinal": 33,
    },
]

test_set2_variables = [
    {
        "name": "STUDYID",
        "role": VariableRoles.IDENTIFIER.value,
        "ordinal": 1,
    },
    {
        "name": "DOMAIN",
        "role": VariableRoles.IDENTIFIER.value,
        "ordinal": 2,
    },
    {
        "name": "USUBJID",
        "role": VariableRoles.IDENTIFIER.value,
        "ordinal": 3,
    },
    {
        "name": "AETERM",
        "role": VariableRoles.IDENTIFIER.value,
        "ordinal": 4,
    },
    {
        "name": "VISITNUM",
        "role": VariableRoles.TIMING.value,
        "ordinal": 17,
    },
    {
        "name": "VISIT",
        "role": VariableRoles.TIMING.value,
        "ordinal": 18,
    },
    {
        "name": "TIMING_VAR",
        "role": VariableRoles.TIMING.value,
        "ordinal": 33,
    },
]


@pytest.mark.parametrize(
    "mock_variables, key_value, expected",
    [
        # Test set 1: Timing variables
        (
            test_set1_variables,
            "Timing",
            ["VISITNUM", "VISIT", "TIMING_VAR"],
        ),
        # Test set 2: Identifier variables
        (
            test_set2_variables,
            "Identifier",
            ["STUDYID", "DOMAIN", "USUBJID", "AETERM"],
        ),
        # Test with wildcard replacement - similar to original tests
        (
            [
                {"name": "--TERM", "role": "Topic", "ordinal": 1},
                {"name": "STUDYID", "role": "Identifier", "ordinal": 2},
            ],
            "Topic",
            ["AETERM"],  # --TERM becomes AETERM for AE domain
        ),
        # Test no matches
        (
            test_set1_variables,
            "NonExistentRole",
            [],
        ),
        # Test empty key/value
        (
            test_set1_variables,
            "",
            [],
        ),
    ],
)
def test_get_model_filtered_variables(mock_variables, key_value, expected):
    """Test get_model_filtered_variables operation with different filter criteria"""
    data_service = PostgresQLDataService.instance()

    # Add test dataset matching original test structure
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="AE",
        column_data={
            "STUDYID": ["TEST_STUDY", "TEST_STUDY", "TEST_STUDY"],
            "AETERM": ["test", "test", "test"],
        },
        standards_context=DefaultStandardsContext(),
    )

    params = SqlOperationParams(
        domain="AE", target=None, standards_context=DefaultStandardsContext(), key_name="role", key_value=key_value
    )

    operation = SqlOperationsFactory.get_service("get_model_filtered_variables", params, data_service)

    # Mock the metadata retrieval method on the operation instance
    with patch.object(operation, "_get_variables_metadata_from_standard_model", return_value=mock_variables):
        result = operation.execute()
        assert_operation_collection(operation, result, expected)


def test_get_model_filtered_variables_exception_handling():
    """Test get_model_filtered_variables when metadata retrieval fails"""
    data_service = PostgresQLDataService.instance()

    # Add test dataset matching original test structure
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="AE",
        column_data={
            "STUDYID": ["TEST_STUDY", "TEST_STUDY", "TEST_STUDY"],
            "AETERM": ["test", "test", "test"],
        },
        standards_context=DefaultStandardsContext(),
    )

    params = SqlOperationParams(
        domain="AE", target=None, standards_context=DefaultStandardsContext(), key_name="role", key_value="Topic"
    )

    operation = SqlOperationsFactory.get_service("get_model_filtered_variables", params, data_service)

    # Mock the metadata method to raise an exception
    with patch.object(
        operation, "_get_variables_metadata_from_standard_model", side_effect=Exception("Metadata retrieval failed")
    ):
        result = operation.execute()

        # Should return empty collection on exception
        assert_operation_collection(operation, result, [])
