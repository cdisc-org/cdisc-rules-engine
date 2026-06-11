from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext
from .helpers import assert_operation_constant
import pytest
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)


class DummyStandardsContext(DefaultStandardsContext):
    def get_domain_metadata(self, domain):
        if domain == "AE":
            return {"filename": "ae.xpt", "name": "AE", "domain": "AE", "size": "5GB"}
        else:
            return {}


def test_dataset_name_extract_metadata():
    data_service = PostgresQLDataService.instance()
    standards_context = DummyStandardsContext()
    params = SqlOperationParams(domain="AE", target="dataset_name", standards_context=standards_context)
    operation = SqlOperationsFactory.get_service("extract_metadata", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, "AE")


def test_size_extract_metadata():
    data_service = PostgresQLDataService.instance()
    standards_context = DummyStandardsContext()
    params = SqlOperationParams(domain="AE", target="size", standards_context=standards_context)
    operation = SqlOperationsFactory.get_service("extract_metadata", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, "5GB")


def test_extract_metadata_exception_handling():
    """Test extract_metadata errors when target metadata not present (eg weight)"""
    data_service = PostgresQLDataService.instance()
    standards_context = DummyStandardsContext()
    params = SqlOperationParams(domain="AE", target="weight", standards_context=standards_context)
    operation = SqlOperationsFactory.get_service("extract_metadata", params, data_service)
    with pytest.raises(Exception):
        operation.execute()
