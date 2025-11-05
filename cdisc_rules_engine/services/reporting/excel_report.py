import logging
from typing import BinaryIO, override
import os

from openpyxl import Workbook
from cdisc_rules_engine.enums.report_types import ReportTypes
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.services.reporting.base_report_data import (
    BaseReportData,
)
from .base_report import BaseReport
from .excel_writer import (
    excel_open_workbook,
    excel_update_worksheet,
    excel_workbook_to_stream,
)


class ExcelReport(BaseReport):
    """
    Generates an excel report for a given set of validation results.
    """

    DEFAULT_MAX_ROWS = 10000

    def __init__(
        self,
        report_standard: BaseReportData,
        args: Validation_args,
        template: BinaryIO | None = None,
    ):
        super().__init__(
            report_standard,
            args,
            template,
        )
        env_max_rows = (
            int(os.getenv("MAX_REPORT_ROWS")) if os.getenv("MAX_REPORT_ROWS") else None
        )
        if env_max_rows is not None and args.max_report_rows is not None:
            result = max(env_max_rows, args.max_report_rows)
        elif env_max_rows is not None:
            result = env_max_rows
        elif args.max_report_rows is not None:
            result = args.max_report_rows
        else:
            result = self.DEFAULT_MAX_ROWS
        if result == 0:
            result = None
        elif result < 0:
            result = self.DEFAULT_MAX_ROWS
        self.max_rows_per_sheet = result
        for report_metadata_item in report_standard.data_sheets["Conformance Details"]:
            if report_metadata_item.name == "Issue Limit Per Sheet":
                report_metadata_item.value = str(self.max_rows_per_sheet)
                break

    @property
    @override
    def _file_ext(self):
        return ReportTypes.XLSX.value.lower()

    def get_export(
        self,
    ) -> Workbook:
        logger = logging.getLogger("validator")
        template_buffer = self._template.read()
        wb = excel_open_workbook(template_buffer)
        for sheet_name, data_sheet in self._report_standard.data_sheets.items():
            total_rows = len(data_sheet)
            if (
                self.max_rows_per_sheet is not None
                and total_rows > self.max_rows_per_sheet
            ):
                data_sheet = data_sheet[: self.max_rows_per_sheet]
                logger.warning(
                    f"{sheet_name} truncated to limit of {self.max_rows_per_sheet} rows. "
                    f"Total issues found: {total_rows}"
                )
            excel_update_worksheet(wb[sheet_name], data_sheet, {"wrap_text": True})
        return wb

    @override
    def write_report(self):
        logger = logging.getLogger("validator")
        try:
            report_data = self.get_export()
            with open(self._output_name, "wb") as f:
                f.write(excel_workbook_to_stream(report_data))
            logger.debug(f"Report written to: {self._output_name}")
        except Exception as e:
            logger.error(f"Error writing report: {e}")
            raise e
        finally:
            self._template.close()
