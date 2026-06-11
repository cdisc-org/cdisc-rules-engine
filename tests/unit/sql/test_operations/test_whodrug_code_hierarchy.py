import pytest
from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.models.sql_external_dictionaries_container import SqlExternalDictionariesContainer
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)

from .helpers import (
    assert_operation_collection,
)


@pytest.mark.parametrize(
    "domain, data, result",
    [
        (
            "CM",
            {
                "CMDECOD": ["DUMMYDRUGNAMEA", "DUMMYDRUGNAMEB"],
                "CMCLAS": ["DUMMYALEVEL4", "DUMMYBLEVEL4"],
                "CMCLASCD": ["A01AA", "B01AA"],
            },
            [True, True],
        ),
        (
            "CM",
            {
                "CMDECOD": ["DUMMYDRUGNAMEC", "DUMMYDRUGNAMED"],
                "CMCLAS": ["DUMMYCLEVEL4", "DUMMYDLEVEL4"],
                "CMCLASCD": ["A01AA", "B01AA"],
            },
            [False, False],
        ),
    ],
)
def test_whodrug_code_hierarchy(sdtm_standards_context, domain, data, result):
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
    params = SqlOperationParams(domain=domain, target=None, standards_context=sdtm_standards_context)
    operation = SqlOperationsFactory.get_service("whodrug_code_hierarchy", params, data_service)
    op_query_result = operation.execute()
    assert_operation_collection(operation, op_query_result, expected=result)
