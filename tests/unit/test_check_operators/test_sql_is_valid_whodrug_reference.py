import pytest
from .helpers import assert_series_equals
from cdisc_rules_engine.check_operators.sql import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.models.sql_external_dictionaries_container import (
    SqlExternalDictionariesContainer,
)


@pytest.mark.parametrize(
    "domain, target, attribute_name, data, result",
    [
        (
            "CM",
            "CMCLASCD",
            "code",
            {
                "CMCLASCD": ["A01AA", "B01AA"],
            },
            [True, True],
        ),
        (
            "CM",
            "CMCLASCD",
            "code",
            {
                "CMCLASCD": ["C01AA", "D01AA"],
            },
            [False, False],
        ),
        (
            "CM",
            "CMDECOD",
            "name",
            {
                "CMDECOD": ["DUMMYDRUGNAMEA", "DUMMYDRUGNAMEB"],
            },
            [True, True],
        ),
        (
            "CM",
            "CMDECOD",
            "name",
            {
                "CMDECOD": ["DUMMYDRUGNAMEC", "DUMMYDRUGNAMED"],
            },
            [False, False],
        ),
        (
            "CM",
            "CMCLAS",
            "class",
            {
                "CMCLAS": ["DUMMYALEVEL4", "DUMMYBLEVEL4"],
            },
            [True, True],
        ),
        (
            "CM",
            "CMCLAS",
            "class",
            {
                "CMCLAS": ["DUMMYCLEVEL4", "DUMMYDLEVEL4"],
            },
            [False, False],
        ),
    ],
)
def test_is_valid_whodrug_reference(sdtm_standards_context, domain, target, attribute_name, data, result):
    data_service = PostgresQLDataService.instance(
        external_dictionaries=SqlExternalDictionariesContainer(
            {DictionaryTypes.WHODRUG.value: "tests/resources/dictionaries/whodrug"}
        )
    )
    PostgresQLDataService.add_test_dataset(
        data_service,
        table_name=domain,
        column_data=data,
        standards_context=sdtm_standards_context,
    )

    config = {"dataset_id": domain, "data_service": data_service}
    op_result = PostgresQLOperators(config).is_valid_whodrug_reference(
        {"target": target, "attribute_name": attribute_name}
    )
    assert_series_equals(op_result, result)
