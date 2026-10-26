import pytest
from .helpers import assert_series_equals, run_operator
from cdisc_rules_engine.check_operators.sql.base_sql_operator import BaseSqlOperator
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.exceptions.custom_exceptions import SqlOperatorError
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
    op_result = run_operator(config, operator, {"target": target, "comparator": comparator})
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
    op_result = run_operator(config, "is_valid_loinc_code_reference", {"target": "LBTESTCD", "comparator": None})
    assert_series_equals(op_result, [False, False])


def _status_loinc_data_service(tmp_path):
    loinc_path = tmp_path / "loinc_2.82"
    loinc_path.mkdir()
    (loinc_path / "Loinc.csv").write_text(
        '"LOINC_NUM","COMPONENT","VersionLastChanged","STATUS"\n'
        '"100000-9","Active term","2.74","ACTIVE"\n'
        '"100001-7","Deprecated term","2.74","DEPRECATED"\n'
    )
    return PostgresQLDataService.instance(
        external_dictionaries=SqlExternalDictionariesContainer({DictionaryTypes.LOINC.value: str(loinc_path)})
    )


@pytest.mark.parametrize(
    "operator, target, comparator, data, result",
    [
        (
            "is_valid_loinc_code_reference",
            "LBLOINC",
            None,
            {"LBLOINC": ["100000-9", "100001-7", "INVALID-CODE"]},
            [False, True, False],
        ),
        (
            "is_not_valid_loinc_code_reference",
            "LBLOINC",
            None,
            {"LBLOINC": ["100000-9", "100001-7", "INVALID-CODE"]},
            [True, False, True],
        ),
        (
            "is_valid_loinc_term_reference",
            "LBTEST",
            None,
            {"LBTEST": ["Active term", "Deprecated term", "INVALID TERM"]},
            [False, True, False],
        ),
        (
            "is_not_valid_loinc_term_reference",
            "LBTEST",
            None,
            {"LBTEST": ["Active term", "Deprecated term", "INVALID TERM"]},
            [True, False, True],
        ),
        (
            "is_valid_loinc_code_term_pair",
            "LBLOINC",
            "LBTEST",
            {
                "LBLOINC": ["100000-9", "100001-7", "100000-9"],
                "LBTEST": ["Active term", "Deprecated term", "Deprecated term"],
            },
            [False, True, False],
        ),
        (
            "is_not_valid_loinc_code_term_pair",
            "LBLOINC",
            "LBTEST",
            {
                "LBLOINC": ["100000-9", "100001-7", "100000-9"],
                "LBTEST": ["Active term", "Deprecated term", "Deprecated term"],
            },
            [True, False, True],
        ),
    ],
)
def test_valid_loinc_references_status_filter(
    sdtm_standards_context, tmp_path, operator, target, comparator, data, result
):
    data_service = _status_loinc_data_service(tmp_path)
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="LB",
        column_data=data,
        standards_context=sdtm_standards_context,
    )

    config = {"dataset_id": "LB", "data_service": data_service}
    op_result = run_operator(
        config,
        operator,
        {"target": target, "comparator": comparator, "filter_attribute": "status", "filter_value": "DEPRECATED"},
    )
    assert_series_equals(op_result, result)


@pytest.mark.parametrize(
    "operator",
    [
        "is_valid_loinc_code_reference",
        "is_not_valid_loinc_code_reference",
        "is_valid_loinc_term_reference",
        "is_not_valid_loinc_term_reference",
        "is_valid_loinc_code_term_pair",
        "is_not_valid_loinc_code_term_pair",
    ],
)
def test_valid_loinc_references_unknown_filter_attribute(sdtm_standards_context, tmp_path, operator):
    data_service = _status_loinc_data_service(tmp_path)
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name="LB",
        column_data={"LBLOINC": ["100000-9"], "LBTEST": ["Active term"]},
        standards_context=sdtm_standards_context,
    )

    config = {"dataset_id": "LB", "data_service": data_service}
    with pytest.raises(SqlOperatorError, match="Filter attribute 'nonexistent' is not a column in ex_loinc"):
        run_operator(
            config,
            operator,
            {"target": "LBLOINC", "comparator": "LBTEST", "filter_attribute": "nonexistent", "filter_value": "X"},
        )
