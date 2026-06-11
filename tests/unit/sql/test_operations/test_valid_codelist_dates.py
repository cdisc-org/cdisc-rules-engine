from cdisc_rules_engine.data_service.postgresql_data_service import (
    PostgresQLDataService,
)
from cdisc_rules_engine.models.sql_operation_params import SqlOperationParams
from cdisc_rules_engine.sql_operations.sql_operations_factory import (
    SqlOperationsFactory,
)
from cdisc_rules_engine.standards.default_standards_context import (
    DefaultStandardsContext,
)

from .helpers import (
    assert_operation_collection,
)


class DummyStandardsContext(DefaultStandardsContext):

    @property
    def standard(self):
        return "SDTMIG"

    def get_ct_packages(self):
        return {
            "define-xmlct-2021-06-25",
            "sdtmct-2018-09-30",
            "sendct-2015-09-25",
            "adamct-2025-03-28",
            "ddfct-2024-09-27",
            "glossaryct-2024-09-27",
            "cdashct-2021-06-25",
            "protocolct-2020-06-26",
            "qrsct-2015-09-25",
            "tmfct-2024-09-27",
            "sdtmct-2018-09-29",
        }


def test_valid_codelist_dates_with_ct_package_types():
    data_service = PostgresQLDataService.instance()
    params = SqlOperationParams(
        domain="AE", target=None, standards_context=DummyStandardsContext(), ct_package_types=["SDTM"]
    )
    operation = SqlOperationsFactory.get_service("valid_codelist_dates", params, data_service)
    result = operation.execute()
    assert_operation_collection(operation, result, ["2018-09-29", "2018-09-30"])


def test_valid_codelist_dates_without_ct_package_types():
    data_service = PostgresQLDataService.instance()
    params = SqlOperationParams(domain="AE", target=None, standards_context=DummyStandardsContext())
    operation = SqlOperationsFactory.get_service("valid_codelist_dates", params, data_service)
    result = operation.execute()
    assert_operation_collection(operation, result, ["2018-09-29", "2018-09-30"])
