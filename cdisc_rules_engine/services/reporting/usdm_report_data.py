from datetime import datetime
from typing import BinaryIO, Iterable
import os

from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.services.reporting.base_report_data import (
    BaseReportData,
)
from cdisc_rules_engine.services.reporting.report_metadata_item import (
    ReportMetadataItem,
)
from version import __version__
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata


class USDMReportData(BaseReportData):
    """
    Report details specific to USDM
    """

    TEMPLATE_FILE_PATH = DefaultFilePaths.USDM_EXCEL_TEMPLATE_FILE.value

    def __init__(
        self,
        datasets: Iterable[SDTMDatasetMetadata],
        dataset_paths: Iterable[str],
        validation_results: list[RuleValidationResult],
        elapsed_time: float,
        args: Validation_args,
        template: BinaryIO | None = None,
        **kwargs,
    ):
        super().__init__(
            datasets,
            dataset_paths,
            validation_results,
            elapsed_time,
            args,
            template,
            **kwargs,
        )
        self.data_sheets = {
            "Conformance Details": self.get_conformance_details_data(),
            "Entity Details": self.get_entity_details_data(),
            "Issue Summary": self.get_summary_data(),
            "Issue Details": self.get_detailed_data(),
            "Rules Report": self.get_rules_report_data(),
        }

    def get_conformance_details_data(
        self,
        file_part: int = None,
        total_parts: int = None,
        **kwargs,
    ) -> list[ReportMetadataItem]:
        timestamp = datetime.now().replace(microsecond=0).isoformat()
        if file_part and total_parts:
            timestamp += f" (Part {file_part} of {total_parts})"
        conformance_details = []
        conformance_details.append(
            ReportMetadataItem("Report Generation", 2, timestamp)
        )
        conformance_details.append(
            ReportMetadataItem(
                "Total Runtime", 3, f"{round(self._elapsed_time, 2)} seconds"
            )
        )
        conformance_details.append(
            ReportMetadataItem("CORE Engine Version", 4, __version__)
        )
        conformance_details.append(ReportMetadataItem("Standard", 7, self._standard))
        conformance_details.append(
            ReportMetadataItem("Version", 8, f"V{self._version}")
        )
        if self._datasets:
            conformance_details.append(
                ReportMetadataItem(
                    "JSON file name",
                    9,
                    os.path.basename(os.path.dirname(self._datasets[0].full_path)),
                )
            )
            conformance_details.append(
                ReportMetadataItem(
                    "Modified Time Stamp", 10, self._datasets[0].modification_date
                )
            )
        return conformance_details

    def get_entity_details_data(self) -> list[dict]:
        entity_details_data = [
            {
                "entity": dataset.name,
                "number_of_instances": dataset.record_count,
            }
            for dataset in self._datasets
        ]
        return sorted(
            entity_details_data,
            key=lambda x: x.get("entity", "").lower(),
        )

    def get_summary_data(self) -> list[dict]:
        """
        Generates the Issue Summary data that goes into the export.
        Each row is represented by a list or a dict containing the following
        information:
        return [
            "Entity",
            "CORE-ID",
            "CDISC RuleID",
            "Message",
            "Issues",
            "Explanation"
        ]
        """
        summary_data = []
        for validation_result in self._results:
            if validation_result.execution_status == "success":
                for result in validation_result.results or []:
                    if (
                        result.get("errors")
                        and result.get("executionStatus") == "success"
                    ):
                        summary_item = {
                            "entity": result.get("entity")
                            or (result.get("dataset", "") or "").replace(".json", ""),
                            "core_id": validation_result.id,
                            "cdisc_rule_id": validation_result.cdisc_rule_id,
                            "message": result.get("message"),
                            "issues": len(result.get("errors")),
                        }
                        summary_data.append(summary_item)

        return sorted(
            summary_data,
            key=lambda x: (x.get("entity"), x["core_id"]),
        )

    def get_detailed_data(self, excel=False) -> list[dict]:
        detailed_data = []
        for validation_result in self._results:
            detailed_data = detailed_data + self._generate_error_details(
                validation_result, excel
            )
        return sorted(
            detailed_data,
            key=lambda x: (x["core_id"], x.get("entity")),
        )

    def _generate_error_details(
        self, validation_result: RuleValidationResult, excel
    ) -> list[dict]:
        """
        Generates the Issue details data that goes into the excel export.
        Each row is represented by a list or a dict containing the following
        information:
        return [
            "CORE-ID",
            "CDISC RuleID",
            "Message",
            "Executability",
            "Entity",
            "Instance ID",
            "Path",
            "Attributes",
            "Value(s)"
        ]
        """
        errors = []
        for result in validation_result.results or []:
            if result.get("errors", []) and result.get("executionStatus") == "success":
                variables = result.get("variables", [])
                for error in result.get("errors"):
                    values = []
                    for variable in variables:
                        raw_value = error.get("value", {}).get(variable)
                        if raw_value is None:
                            values.append(None)
                        else:
                            values.append(str(raw_value))
                    error_item = {
                        "core_id": validation_result.id,
                        "cdisc_rule_id": validation_result.cdisc_rule_id,
                        "message": result.get("message"),
                        "executability": validation_result.executability,
                        "entity": error.get("entity")
                        or error.get("dataset", "").replace(".json", ""),
                        "instance_id": error.get("instance_id"),
                        "path": error.get("path"),
                        "attributes": variables,
                        "values": self.process_values(values),
                    }
                    errors.append(error_item)
        return errors

    def get_rules_report_data(self) -> list[dict]:
        """
        Generates the rules report data that goes into the excel export.
        Each row is represented by a list or a dict containing the following
        information:
        [
            "CORE-ID",
            "Version",
            "CDISC RuleID",
            "Message",
            "Status"
        ]
        """
        rules_report = []
        for validation_result in self._results:
            rules_item = {
                "core_id": validation_result.id,
                "version": "1",
                "cdisc_rule_id": validation_result.cdisc_rule_id,
                "message": validation_result.message,
                "status": (
                    ExecutionStatus.SUCCESS.value.upper()
                    if validation_result.execution_status
                    == ExecutionStatus.SUCCESS.value
                    else ExecutionStatus.SKIPPED.value.upper()
                ),
            }
            rules_report.append(rules_item)

        return sorted(
            rules_report,
            key=lambda x: x["core_id"],
        )
