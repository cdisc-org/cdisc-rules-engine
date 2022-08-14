import logging
import json
from collections import namedtuple
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.services.data_services.local_data_service import (
    LocalDataService,
)
from cdisc_rules_engine.utilities.excel_report import ExcelReport
from cdisc_rules_engine.utilities.excel_writer import excel_workbook_to_stream
from cdisc_rules_engine.utilities.json_report import JsonReport


def write_report(
    data_path: str,
    results: list[RuleValidationResult],
    elapsed_time: float,
    data_service: LocalDataService,
    args: namedtuple,
):

    output_name = args.output + '.' + args.output_format.lower()
    logger = logging.getLogger("validator")
    if (args.output_format.lower() == "json"):
        report = JsonReport(data_path, results, elapsed_time, None, "dict")
        report_data = report.get_json_export(
            args.define_version,
            args.controlled_terminology_package,
            args.standard,
            args.version.replace("-", "."),
            args.raw_report,
        )
        with open(output_name, "w") as f:
            json.dump(report_data, f)
    elif (args.output_format.lower() == "xlsx"):
        report_template = data_service.read_data(args.report_template, "rb")
        report = ExcelReport(data_path, results, elapsed_time, report_template.read())
        try:
            report_data = report.get_excel_export(
                args.define_version,
                args.controlled_terminology_package,
                args.standard,
                args.version.replace("-", "."),
            )
            with open(output_name, "wb") as f:
                f.write(excel_workbook_to_stream(report_data))
        except Exception as e:
            logger.error(e)
            raise e
        finally:
            report_template.close()
