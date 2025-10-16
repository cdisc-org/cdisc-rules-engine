from unittest.mock import MagicMock

from cdisc_rules_engine.enums.report_types import ReportTypes
from cdisc_rules_engine.services.reporting import ReportFactory
from cdisc_rules_engine.services.reporting.excel_report import ExcelReport
from cdisc_rules_engine.services.reporting.json_report import JsonReport


def test_get_report_services():
    """
    Unit test for ReportFactory.get_report_services
    """
    factory = ReportFactory(
        datasets=[],
        results=[],
        elapsed_time=10.5,
        args=MagicMock(
            output_format=ReportTypes.values(),
            max_report_rows=None,
            max_errors_per_rule=(None, False),
        ),
        data_service=MagicMock(),
    )
    services = factory.get_report_services()
    assert len(services) == 2
    for service in services:
        is_excel: bool = isinstance(service, ExcelReport) and not isinstance(
            service, JsonReport
        )
        is_json: bool = isinstance(service, JsonReport) and not isinstance(
            service, ExcelReport
        )
        assert is_excel or is_json
