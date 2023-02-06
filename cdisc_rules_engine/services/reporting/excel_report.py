import logging
from datetime import datetime
from typing import BinaryIO, List, Optional, Iterable

from openpyxl import Workbook
from openpyxl.styles import Alignment

from cdisc_rules_engine.enums.report_types import ReportTypes
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.models.validation_args import Validation_args
from .base_report import BaseReport
from .excel_writer import (
    excel_open_workbook,
    excel_update_worksheet,
    excel_workbook_to_stream,
)
from cdisc_rules_engine.utilities.reporting_utilities import (
    get_define_version,
)


class ExcelReport(BaseReport):
    """
    Generates an excel report for a given set of validation results.
    """

    def __init__(
        self,
        dataset_paths: Iterable[str],
        validation_results: List[RuleValidationResult],
        elapsed_time: float,
        args: Validation_args,
        template: Optional[BinaryIO] = None,
    ):
        super().__init__(
            dataset_paths, validation_results, elapsed_time, args, template
        )
        self._item_type = "list"

    @property
    def _file_format(self):
        return ReportTypes.XLSX.value.lower()

    def get_export(
        self, define_version, cdiscCt, standard, version, **kwargs
    ) -> Workbook:
        wb = excel_open_workbook(self._template.read())
        summary_data = self.get_summary_data()
        detailed_data = self.get_detailed_data()
        rules_report_data = self.get_rules_report_data()
        excel_update_worksheet(wb["Issue Summary"], summary_data, dict(wrap_text=True))
        excel_update_worksheet(wb["Issue Details"], detailed_data, dict(wrap_text=True))
        excel_update_worksheet(
            wb["Rules Report"], rules_report_data, dict(wrap_text=True)
        )
        wb["Conformance Details"]["B2"] = ",\n".join(self._dataset_paths[:5])
        wb["Conformance Details"]["B2"].alignment = Alignment(wrapText=True)
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
        wb["Conformance Details"]["B14"] = self._args.meddra
        wb["Conformance Details"]["B15"] = self._args.whodrug
        return wb

    def write_report(self):
        logger = logging.getLogger("validator")

        try:
            define_version: str = self._args.define_version or get_define_version(
                self._args.dataset_paths
            )
            report_data = self.get_export(
                define_version,
                self._args.controlled_terminology_package,
                self._args.standard,
                self._args.version.replace("-", "."),
            )
            with open(self._output_name, "wb") as f:
                f.write(excel_workbook_to_stream(report_data))
        except Exception as e:
            logger.error(e)
            raise e
        finally:
            self._template.close()
