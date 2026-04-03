import pytest
from .helpers import assert_series_equals
from cdisc_rules_engine.check_operators.sql import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.models.sql_external_dictionaries_container import (
    SqlExternalDictionariesContainer,
)


@pytest.mark.parametrize(
    "domain, target, data, result",
    [
        (
            "AE",
            "AESOCCD",
            {
                "AESOCCD": ["SOC1", "SOC2", "INVALIDSOC"],
            },
            [True, True, False],
        ),
        (
            "AE",
            "AEPTCD",
            {
                "AEPTCD": ["PT1", "PT2", "INVALIDPT"],
            },
            [True, True, False],
        ),
        (
            "MH",
            "MHLLTCD",
            {
                "MHLLTCD": ["LLT1", "LLT2", "INVALIDLLT"],
            },
            [True, True, False],
        ),
    ],
)
def test_valid_meddra_code_references(sdtm_standards_context, domain, target, data, result):
    data_service = PostgresQLDataService.instance(
        external_dictionaries=SqlExternalDictionariesContainer(
            {DictionaryTypes.MEDDRA.value: "tests/resources/dictionaries/meddra"}
        )
    )
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name=domain,
        column_data=data,
        standards_context=sdtm_standards_context,
    )

    config = {"dataset_id": domain, "data_service": data_service}
    op_result = PostgresQLOperators(config).is_valid_meddra_code_reference({"target": target})
    assert_series_equals(op_result, result)


@pytest.mark.parametrize(
    "domain, target, data, result",
    [
        (
            "AE",
            "AESOC",
            {
                "AESOC": ["TESTSOC1", "TESTSOC2", "INVALID SOC TERM"],
            },
            [True, True, False],
        ),
        (
            "AE",
            "AEDECOD",
            {
                "AEDECOD": ["TESTPT1", "TESTPT2", "INVALID PT TERM"],
            },
            [True, True, False],
        ),
        (
            "MH",
            "MHHLGT",
            {
                "MHHLGT": ["TESTHLGT1", "TESTHLGT2", "INVALID HLGT TERM"],
            },
            [True, True, False],
        ),
    ],
)
def test_valid_meddra_term_references(sdtm_standards_context, domain, target, data, result):
    data_service = PostgresQLDataService.instance(
        external_dictionaries=SqlExternalDictionariesContainer(
            {DictionaryTypes.MEDDRA.value: "tests/resources/dictionaries/meddra"}
        )
    )
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name=domain,
        column_data=data,
        standards_context=sdtm_standards_context,
    )

    config = {"dataset_id": domain, "data_service": data_service}
    op_result = PostgresQLOperators(config).is_valid_meddra_term_reference({"target": target})
    assert_series_equals(op_result, result)


@pytest.mark.parametrize(
    "domain, target, data, result",
    [
        (
            "AE",
            "AEPTCD",
            {
                "AEPTCD": ["PT1", "PT2", "PT3"],
                "AEDECOD": ["TESTPT1", "TESTPT2", "INVALID PT TERM"],
            },
            [True, True, False],
        ),
        (
            "AE",
            "AEDECOD",
            {
                "AEPTCD": ["PT1", "PT2", "INVALIDPT"],
                "AEDECOD": ["TESTPT1", "TESTPT2", "TESTPT3"],
            },
            [True, True, False],
        ),
        (
            "CE",
            "CESOCCD",
            {
                "CESOCCD": ["SOC1", "SOC2", "INVALID SOC CODE"],
                "CESOC": ["TESTSOC1", "TESTSOC2", "INVALID SOC TERM"],
            },
            [True, True, False],
        ),
    ],
)
def test_valid_meddra_code_term_pairs(sdtm_standards_context, domain, target, data, result):
    data_service = PostgresQLDataService.instance(
        external_dictionaries=SqlExternalDictionariesContainer(
            {DictionaryTypes.MEDDRA.value: "tests/resources/dictionaries/meddra"}
        )
    )
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name=domain,
        column_data=data,
        standards_context=sdtm_standards_context,
    )

    config = {"dataset_id": domain, "data_service": data_service}
    op_result = PostgresQLOperators(config).is_valid_meddra_code_term_pair({"target": target})
    assert_series_equals(op_result, result)
