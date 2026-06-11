import pytest
from .helpers import assert_series_equals
from cdisc_rules_engine.check_operators.sql import PostgresQLOperators
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.models.sql_external_dictionaries_container import (
    SqlExternalDictionariesContainer,
)


@pytest.mark.parametrize(
    "operator, domain, target, comparator, data, result",
    [
        (
            "is_valid_unii_code_reference",
            "MH",
            "MHTRTCD",
            None,
            {
                "MHTRTCD": ["0001H6R5H1", "0009999999"],
            },
            [True, False],
        ),
        (
            "is_valid_unii_term_reference",
            "MH",
            "MHTRT",
            None,
            {
                "MHTRT": ["CEROUS SALICYLATE", "INVALID UNII TERM"],
            },
            [True, False],
        ),
        (
            "is_valid_unii_code_term_pair",
            "MH",
            "MHTRTCD",
            "MHTRT",
            {
                "MHTRTCD": ["0001H6R5H1", "N0000180850", "0009999999"],
                "MHTRT": ["CEROUS SALICYLATE", "DI(DEHYDROABIETYL)AMINE ACETATE", "INVALID UNII TERM"],
            },
            [True, False, False],
        ),
    ],
)
def test_valid_unii_references(sdtm_standards_context, operator, domain, target, comparator, data, result):
    data_service = PostgresQLDataService.instance(
        external_dictionaries=SqlExternalDictionariesContainer(
            {DictionaryTypes.UNII.value: "tests/resources/dictionaries/unii"}
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
