import json
from typing import BinaryIO, override

from cdisc_rules_engine.enums.report_types import ReportTypes
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.services.reporting.base_report_data import (
    BaseReportData,
)
from cdisc_rules_engine.services.reporting.report_metadata_item import (
    ReportMetadataItem,
)
from .base_report import BaseReport


class JsonReport(BaseReport):
    """
    Generates a json report for a given set of validation results.
    """

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

    @property
    @override
    def _file_ext(self) -> str:
        return ReportTypes.JSON.value.lower()

    @staticmethod
    def _get_property_name(name: str) -> str:
        return name.replace(" ", "_").replace("-", "_")

    def get_export(
        self,
        raw_report=False,
    ) -> dict:
        json_export = {}
        for sheet_name, data_sheet in self._report_standard.data_sheets.items():
            if (
                isinstance(data_sheet, list)
                and data_sheet
                and isinstance(data_sheet[0], ReportMetadataItem)
            ):
                json_export[self._get_property_name(sheet_name)] = {
                    self._get_property_name(item.name): item.value
                    for item in data_sheet
                }
            else:
                json_export[self._get_property_name(sheet_name)] = data_sheet
        if raw_report:
            json_export["results_data"] = [
                rule_result.to_representation() for rule_result in self._results
            ]
        return json_export

    @override
    def write_report(self):
        report_data = self.get_export(
            raw_report=self._args.raw_report,
        )
        with open(self._output_name, "w") as f:
            json.dump(report_data, f)
