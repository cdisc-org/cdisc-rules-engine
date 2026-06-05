import csv
import os
from io import IOBase
from typing import override

from cdisc_rules_engine.enums.report_types import ReportTypes
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.services.reporting.base_report_data import BaseReportData

from .base_report import BaseReport


class CsvReport(BaseReport):
    """
    Writes a results.csv file in the format defined by the report standard,
    compatible with the cdisc-open-rules test harness baselines.
    """

    def __init__(
        self,
        report_standard: BaseReportData,
        args: Validation_args,
        template: IOBase | None = None,
    ):
        super().__init__(report_standard, args, template)

    @property
    @override
    def _file_ext(self) -> str:
        return ReportTypes.CSV.value.lower()

    @override
    def write_report(self) -> None:
        output_dir = os.path.dirname(self._output_name)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        header, rows = self._report_standard.get_csv_rows()

        with open(self._output_name, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(header)
            writer.writerows(rows)
