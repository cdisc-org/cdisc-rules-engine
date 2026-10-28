from unittest.mock import MagicMock

from cdisc_rules_engine.enums.report_types import ReportTypes
from cdisc_rules_engine.services.reporting import ReportFactory
from cdisc_rules_engine.services.reporting.csv_report import CsvReport
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
        args=MagicMock(output_format=ReportTypes.values()),
        data_service=MagicMock(),
    )
    services = factory.get_report_services()
    assert sorted(type(service).__name__ for service in services) == sorted(
        [CsvReport.__name__, ExcelReport.__name__, JsonReport.__name__]
    )
