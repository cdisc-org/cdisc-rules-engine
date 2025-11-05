from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext
from .helpers import (
    assert_operation_collection,
)
import pytest
from unittest.mock import patch
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)


class DummyDatasetMetadata:
    def __init__(self, filename=None, name=None, domain=None):
        self.filename = filename
        self.name = name
        self.domain = domain


test_dataset_metadata = [
    DummyDatasetMetadata(
        filename="ae.xpt",
        name="AE",
        domain="AE",
    ),
    DummyDatasetMetadata(
        filename="supplb.xpt",
        name="SUPPLB",
        domain="SUPPQUAL",
    ),
]


@pytest.mark.parametrize(
    "mock_datasets, expected",
    [
        (
            test_dataset_metadata,
            ["AE", "SUPPQUAL"],
        ),
    ],
)
def test_study_domains(mock_datasets, expected):
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

    operation = SqlOperationsFactory.get_service("study_domains", params, data_service)

    # Mock the metadata retrieval method on the operation instance
    with patch.object(
        operation.__class__,
        "_get_full_dataset_metadata",
        return_value=mock_datasets,
    ):
        result = operation.execute()
        assert_operation_collection(operation, result, expected, unsorted=True)


test_duplicate_dataset_metadata = [
    DummyDatasetMetadata(
        filename="dm.xpt",
        name="DM",
        domain="DM",
    ),
    DummyDatasetMetadata(
        filename="dm1.xpt",
        name="DM",
        domain="DM",
    ),
    DummyDatasetMetadata(
        filename="ae.xpt",
        name="AE",
        domain="AE",
    ),
    DummyDatasetMetadata(
        filename="tv.xpt",
        name="TV",
        domain="TV",
    ),
]


@pytest.mark.parametrize(
    "mock_datasets, expected",
    [
        (
            test_duplicate_dataset_metadata,
            ["AE", "DM", "TV"],
        ),
    ],
)
def test_duplicate_study_domains(mock_datasets, expected):
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

    operation = SqlOperationsFactory.get_service("study_domains", params, data_service)

    # Mock the metadata retrieval method on the operation instance
    with patch.object(
        operation.__class__,
        "_get_full_dataset_metadata",
        return_value=mock_datasets,
    ):
        result = operation.execute()
        assert_operation_collection(operation, result, expected, unsorted=True)


test_missing_domain_dataset_metadata = [
    DummyDatasetMetadata(
        filename="dm.xpt",
    ),
    DummyDatasetMetadata(
        filename="dm1.xpt",
        name="DM",
        domain="DM",
    ),
    DummyDatasetMetadata(
        filename="ae.xpt",
        name="AE",
        domain="AE",
    ),
    DummyDatasetMetadata(
        filename="tv.xpt",
        name="TV",
        domain="TV",
    ),
]


@pytest.mark.parametrize(
    "mock_datasets, expected",
    [
        (
            test_missing_domain_dataset_metadata,
            ["", "AE", "DM", "TV"],
        ),
    ],
)
def test_missing_domain_study_domains(mock_datasets, expected):
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

    operation = SqlOperationsFactory.get_service("study_domains", params, data_service)

    # Mock the metadata retrieval method on the operation instance
    with patch.object(
        operation.__class__,
        "_get_full_dataset_metadata",
        return_value=mock_datasets,
    ):
        result = operation.execute()
        assert_operation_collection(operation, result, expected, unsorted=True)
