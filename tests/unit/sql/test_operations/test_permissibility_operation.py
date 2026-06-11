from unittest.mock import patch

import pytest

from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
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


test_set1_variables = [
    {
        "name": "STUDYID",
        "core": "Req",
        "ordinal": 1,
    },
    {
        "name": "DOMAIN",
        "core": "Req",
        "ordinal": 2,
    },
    {
        "name": "USUBJID",
        "core": "Req",
        "ordinal": 3,
    },
    {
        "name": "AESTDTC",
        "core": "Exp",
        "ordinal": 4,
    },
    {
        "name": "AETERM",
        "core": "Exp",
        "ordinal": 17,
    },
    {
        "name": "AESCAN",
        "core": "Perm",
        "ordinal": 18,
    },
    {
        "name": "AESCONG",
        "core": "Perm",
        "ordinal": 33,
    },
]


@pytest.mark.parametrize(
    "mock_variables, op, expected",
    [
        (
            test_set1_variables,
            "required_variables",
            ["STUDYID", "DOMAIN", "USUBJID"],
        ),
        (
            test_set1_variables,
            "expected_variables",
            ["AESTDTC", "AETERM"],
        ),
        (
            test_set1_variables,
            "permissible_variables",
            ["AESCAN", "AESCONG"],
        ),
    ],
)
def test_permissibility_operation(mock_variables, op, expected):
    """Test permissibility operations with different filter criteria"""
    data_service = PostgresQLDataService.instance()
    standards_context = DefaultStandardsContext()

    # Add test dataset matching original test structure
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="AE",
        column_data={
            "STUDYID": ["TEST_STUDY", "TEST_STUDY", "TEST_STUDY"],
            "AETERM": ["test", "test", "test"],
        },
        standards_context=standards_context,
    )

    params = SqlOperationParams(domain="AE", target=None, standards_context=standards_context)

    operation = SqlOperationsFactory.get_service(op, params, data_service)

    # Mock the metadata retrieval method on the operation instance
    with patch.object(
        operation,
        "_get_variables_metadata_from_standard",
        return_value=mock_variables,
    ):
        result = operation.execute()
        assert_operation_collection(operation, result, expected)


@pytest.mark.parametrize(
    "op",
    [
        ("required_variables"),
        ("expected_variables"),
        ("permissible_variables"),
    ],
)
def test_permissibility_operation_exception_handling(op):
    """Test permissibility operations when metadata retrieval fails"""
    data_service = PostgresQLDataService.instance()
    standards_context = DefaultStandardsContext()

    # Add test dataset matching original test structure
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="AE",
        column_data={
            "STUDYID": ["TEST_STUDY", "TEST_STUDY", "TEST_STUDY"],
            "AETERM": ["test", "test", "test"],
        },
        standards_context=standards_context,
    )

    params = SqlOperationParams(domain="AE", target=None, standards_context=standards_context)

    operation = SqlOperationsFactory.get_service(op, params, data_service)

    # Mock the metadata method to raise an exception
    with patch.object(
        operation,
        "_get_variables_metadata_from_standard",
        side_effect=Exception("Metadata retrieval failed"),
    ):

        with pytest.raises(Exception):
            operation.execute()
