import pytest
from cdisc_rules_engine.data_service.postgresql_data_service import PostgresQLDataService
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.get_countries import (
    SqlGetCountriesOperation,
    ISO_3166_COUNTRY_CODES,
    GENC_COUNTRY_CODES,
)
from .helpers import assert_operation_collection


@pytest.mark.parametrize("attribute", ["country_name", "alpha_2", "alpha_3"])
def test_get_countries_by_attribute(sdtm_standards_context, attribute):
    data_service = PostgresQLDataService.instance()
    params = SqlOperationParams(
        domain="AE", target="FAKEVARIABLE", standards_context=sdtm_standards_context, attribute_name=attribute
    )
    operation = SqlGetCountriesOperation(params=params, data_service=data_service)
    result = operation.execute()

    iso_names = {entry["country_name"] for entry in ISO_3166_COUNTRY_CODES}
    master_list = ISO_3166_COUNTRY_CODES + [
        entry for entry in GENC_COUNTRY_CODES if entry["country_name"] not in iso_names
    ]
    expected = list({entry[attribute] for entry in master_list})

    assert_operation_collection(operation, result, expected=expected, unsorted=True)
