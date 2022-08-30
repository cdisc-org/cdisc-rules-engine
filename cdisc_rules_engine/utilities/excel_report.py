import logging
from datetime import datetime
from typing import List, TextIO
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.utilities.excel_writer import excel_workbook_to_stream
from openpyxl import Workbook
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.utilities.excel_writer import (
    excel_open_workbook,
    excel_update_worksheet,
)
from cdisc_rules_engine.utilities.base_report import BaseReport


class ExcelReport(BaseReport):
    """
    Generates an excel report for a given set of validation results.
    """

    def __init__(
        self,
        data_path: str,
        validation_results: List[RuleValidationResult],
        elapsed_time: float,
        args: Validation_args,
        template: TextIO,
    ):
        super().__init__(data_path, validation_results, elapsed_time, args)
        self._template = template
        self._item_type = "list"

    def get_export(self, define_version, cdiscCt, standard, version) -> Workbook:
        wb = excel_open_workbook(self._template.read())
        summary_data = self.get_summary_data()
        detailed_data = self.get_detailed_data()
        rules_report_data = self.get_rules_report_data()
        excel_update_worksheet(wb["Issue Summary"], summary_data, dict(wrap_text=True))
        excel_update_worksheet(wb["Issue Details"], detailed_data, dict(wrap_text=True))
        excel_update_worksheet(
            wb["Rules Report"], rules_report_data, dict(wrap_text=True)
        )
        wb["Conformance Details"]["B2"] = self._data_path
        # write conformance data
        wb["Conformance Details"]["B3"] = (
            datetime.now().replace(microsecond=0).isoformat()
        )
        wb["Conformance Details"]["B4"] = f"{round(self._elapsed_time, 2)} seconds"
        # write bundle details
        wb["Conformance Details"]["B8"] = standard.upper()
        wb["Conformance Details"]["B9"] = f"V{version}"
        wb["Conformance Details"]["B10"] = ", ".join(cdiscCt)
        wb["Conformance Details"]["B11"] = define_version
        return wb

    def write_report(self):
        output_name = self._args.output + "." + self._args.output_format.lower()
        logger = logging.getLogger("validator")
        try:
            report_data = self.get_export(
                self._args.define_version,
                self._args.controlled_terminology_package,
                self._args.standard,
                self._args.version.replace("-", "."),
            )
            with open(output_name, "wb") as f:
                f.write(excel_workbook_to_stream(report_data))
        except Exception as e:
            logger.error(e)
            raise e
        finally:
            self._template.close()
