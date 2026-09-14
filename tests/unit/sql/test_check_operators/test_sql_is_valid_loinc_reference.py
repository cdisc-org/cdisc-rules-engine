import pytest
from .helpers import assert_series_equals
from cdisc_rules_engine.check_operators.sql import PostgresQLOperators
from cdisc_rules_engine.check_operators.sql.base_sql_operator import BaseSqlOperator
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.models.sql_external_dictionaries_container import (
    SqlExternalDictionariesContainer,
)


@pytest.mark.parametrize(
    "operator, domain, target, comparator, data, result",
    [
        (
            "is_valid_loinc_code_reference",
            "LB",
            "LBTESTCD",
            None,
            {
                "LBTESTCD": ["100000-9", "100001-7", "INVALID-CODE"],
            },
            [True, True, False],
        ),
        (
            "is_valid_loinc_term_reference",
            "LB",
            "LBTEST",
            None,
            {
                "LBTEST": [
                    "Health informatics pioneer and the father of LOINC",
                    "Specimen care is maintained",
                    "INVALID TERM",
                ],
            },
            [True, True, False],
        ),
        (
            "is_valid_loinc_code_term_pair",
            "LB",
            "LBTESTCD",
            "LBTEST",
            {
                "LBTESTCD": ["100000-9", "100002-5", "INVALID-CODE"],
                "LBTEST": [
                    "Health informatics pioneer and the father of LOINC",
                    "Specimen care is maintained",
                    "INVALID TERM",
                ],
            },
            [True, True, False],
        ),
    ],
)
def test_valid_loinc_references(sdtm_standards_context, operator, domain, target, comparator, data, result):
    data_service = PostgresQLDataService.instance(
        external_dictionaries=SqlExternalDictionariesContainer(
            {DictionaryTypes.LOINC.value: "tests/resources/dictionaries/loinc"}
        )
    )
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name=domain,
        column_data=data,
        standards_context=sdtm_standards_context,
    )

    config = {"dataset_id": domain, "data_service": data_service}
    op_result = getattr(PostgresQLOperators(config), operator)({"target": target, "comparator": comparator})
    assert_series_equals(op_result, result)


@pytest.mark.parametrize(
    "column_version, filter_value, expected",
    [
        ("2.10", "2.9", False),
        ("2.9", "2.10", True),
        ("2.72", "2.9", False),
        ("2.72", "2.73", True),
    ],
)
def test_version_le_condition_sql_compares_dotted_versions_numerically(column_version, filter_value, expected):
    data_service = PostgresQLDataService.instance()
    condition = BaseSqlOperator._version_le_condition_sql(f"'{column_version}'", filter_value)
    data_service.pgi.execute_sql(f"SELECT ({condition}) AS result;")
    row = data_service.pgi.fetch_one()
    assert row["result"] == expected


def test_valid_loinc_code_reference_missing_target_column(sdtm_standards_context):
    data_service = PostgresQLDataService.instance(
        external_dictionaries=SqlExternalDictionariesContainer(
            {DictionaryTypes.LOINC.value: "tests/resources/dictionaries/loinc"}
        )
    )
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="LB",
        column_data={"LBOTHER": ["100000-9", "100001-7"]},
        standards_context=sdtm_standards_context,
    )

    config = {"dataset_id": "LB", "data_service": data_service}
    op_result = PostgresQLOperators(config).is_valid_loinc_code_reference({"target": "LBTESTCD", "comparator": None})
    assert_series_equals(op_result, [False, False])
