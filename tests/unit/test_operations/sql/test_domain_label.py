from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.models.test_dataset import TestDataset, TestVariableMetadata
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)

from .helpers import (
    assert_operation_constant,
)


def test_domain_label():
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service, table_name="ae", column_data={"key": [1, 2, 3], "name": ["A", "B", "C"]}
    )

    params = SqlOperationParams(domain="ae", target=None, standard="", standard_version="")
    operation = SqlOperationsFactory.get_service("domain_label", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, "Test ae Dataset")  # Default label for test data


def test_domain_label_nonexistent_domain():
    data_service = PostgresQLDataService.instance()
    PostgresQLDataService.add_test_dataset(
        data_service, table_name="ae", column_data={"key": [1, 2, 3], "name": ["A", "B", "C"]}
    )

    params = SqlOperationParams(domain="vs", target=None, standard="", standard_version="")
    operation = SqlOperationsFactory.get_service("domain_label", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, "")  # Empty label when domain not found


def test_domain_label_with_valid_label():
    ae_dataset = TestDataset(
        filename="ae.xpt",
        filepath="path/to/ae.xpt",
        name="AE",
        label="Adverse Events",
        variables=[
            TestVariableMetadata(
                name="DOMAIN",
                label="Domain Abbreviation",
                type="Char",
                length=4,
                format="",
            ),
            TestVariableMetadata(
                name="AESEQ",
                label="Sequence Number",
                type="Num",
                length=8,
                format="",
            ),
        ],
        records={
            "DOMAIN": ["AE", "AE"],
            "AESEQ": [1, 2],
        },
    )

    data_service = PostgresQLDataService.from_list_of_testdatasets([ae_dataset])

    params = SqlOperationParams(domain="ae", target=None, standard="", standard_version="")
    operation = SqlOperationsFactory.get_service("domain_label", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, "Adverse Events")


def test_domain_label_with_empty_label():
    vs_dataset = TestDataset(
        filename="vs.xpt",
        filepath="path/to/vs.xpt",
        name="VS",
        label="",  # Empty label
        variables=[
            TestVariableMetadata(
                name="DOMAIN",
                label="Domain Abbreviation",
                type="Char",
                length=4,
                format="",
            ),
            TestVariableMetadata(
                name="VSSEQ",
                label="Sequence Number",
                type="Num",
                length=8,
                format="",
            ),
        ],
        records={
            "DOMAIN": ["VS", "VS"],
            "VSSEQ": [1, 2],
        },
    )

    data_service = PostgresQLDataService.from_list_of_testdatasets([vs_dataset])

    params = SqlOperationParams(domain="vs", target=None, standard="", standard_version="")
    operation = SqlOperationsFactory.get_service("domain_label", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, "")


def test_domain_label_with_quotes_in_label():
    lb_dataset = TestDataset(
        filename="lb.xpt",
        filepath="path/to/lb.xpt",
        name="LB",
        label="Laboratory 'Test' Results",  # Label with single quotes
        variables=[
            TestVariableMetadata(
                name="DOMAIN",
                label="Domain Abbreviation",
                type="Char",
                length=4,
                format="",
            ),
            TestVariableMetadata(
                name="LBSEQ",
                label="Sequence Number",
                type="Num",
                length=8,
                format="",
            ),
        ],
        records={
            "DOMAIN": ["LB", "LB"],
            "LBSEQ": [1, 2],
        },
    )

    data_service = PostgresQLDataService.from_list_of_testdatasets([lb_dataset])

    params = SqlOperationParams(domain="lb", target=None, standard="", standard_version="")
    operation = SqlOperationsFactory.get_service("domain_label", params, data_service)
    result = operation.execute()
    assert_operation_constant(operation, result, "Laboratory 'Test' Results")
