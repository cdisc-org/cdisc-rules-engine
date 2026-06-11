from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.standards.default_standards_context import DefaultStandardsContext
from cdisc_rules_engine.sql_operations.sql_operations_factory import SqlOperationsFactory

TEST_DATASET = {
    "DUMMYVAR": ["Severity/Intensity", "Epoch", "DummyLabel"],
}


class DummyStandardsContext(DefaultStandardsContext):
    def get_standard_metadata(self):
        return {
            "classes": [
                {
                    "datasets": [
                        {
                            "datasetVariables": [
                                {"name": "EPOCH", "role": "Timing", "core": "Perm"},
                                {"name": "AESEV", "role": "Record Qualifier", "core": "Perm"},
                            ]
                        }
                    ]
                }
            ]
        }


def test_name_referenced_variable_metadata_role():
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service=data_service, table_name="AE", column_data=TEST_DATASET, standards_context=DummyStandardsContext()
    )

    params = SqlOperationParams(
        domain="AE", target="DUMMYVAR", attribute_name="role", standards_context=DummyStandardsContext()
    )
    operation = SqlOperationsFactory.get_service("name_referenced_variable_metadata", params, data_service)

    result = operation.execute()

    assert result.type == "collection"
    assert result.subtype == "Char"
    assert result.params == {"%target%": "DUMMYVAR"}

    assert result.query.startswith("SELECT val FROM (VALUES ")
    assert "('EPOCH', 'Timing')" in result.query
    assert "('AESEV', 'Record Qualifier')" in result.query
    assert "DummyLabel" not in result.query
    assert result.query.endswith(" AS t(name, val) WHERE t.name = %target%")


def test_name_referenced_variable_metadata_core():
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service=data_service, table_name="AE", column_data=TEST_DATASET, standards_context=DummyStandardsContext()
    )

    params = SqlOperationParams(
        domain="AE", target="DUMMYVAR", attribute_name="core", standards_context=DummyStandardsContext()
    )
    operation = SqlOperationsFactory.get_service("name_referenced_variable_metadata", params, data_service)

    result = operation.execute()

    assert result.type == "collection"
    assert result.subtype == "Char"
    assert result.params == {"%target%": "DUMMYVAR"}

    assert result.query.startswith("SELECT val FROM (VALUES ")
    assert "('EPOCH', 'Perm')" in result.query
    assert "('AESEV', 'Perm')" in result.query
    assert "DummyLabel" not in result.query
    assert result.query.endswith(" AS t(name, val) WHERE t.name = %target%")
