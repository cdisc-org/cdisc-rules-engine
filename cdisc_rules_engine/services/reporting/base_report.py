import re
from abc import ABC, abstractmethod
from io import IOBase
from pathlib import Path
from typing import List, Optional, Union, Iterable

from openpyxl import Workbook

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata


_PER_DATASET_SKIP_REASON = re.compile(r"^(?P<reason>.+?) for rule id=[^,]*, dataset=(?P<dataset>\S+)$")


class BaseReport(ABC):
    """
    Generates a base report for a given set of validation results.
    """

    def __init__(
        self,
        datasets: Iterable[SDTMDatasetMetadata],
        dataset_paths: Iterable[str],
        validation_results: List[RuleValidationResult],
        elapsed_time: float,
        args: Validation_args,
        template: Optional[IOBase] = None,
    ):
        self._datasets = datasets
        self._dataset_paths: Iterable[str] = dataset_paths
        self._elapsed_time: float = elapsed_time
        self._results: List[RuleValidationResult] = validation_results
        self._item_type = ""
        self._args = args
        self._template = template
        self._output_name: str = f"{self._args.output}.{self._file_format}"

    def get_summary_data(self) -> List[List]:
        """
        Generates the Issue Summary data that goes into the export.
        Each row is represented by a list or a dict containing the following
        information:
        return [
            "Dataset",
            "CORE-ID",
            "Message",
            "Issues",
            "Explanation"
        ]
        """
        summary_data = []
        for validation_result in self._results:
            if validation_result.execution_status == "success":
                for result in validation_result.results or []:
                    dataset = result.get("dataset")
                    if result.get("errors") and result.get("executionStatus") == "success":
                        summary_item = {
                            "dataset": dataset,
                            "core_id": validation_result.id,
                            "message": result.get("message"),
                            "issues": len(result.get("errors")),
                        }

                        if self._item_type == "list":
                            summary_data.extend([[*summary_item.values()]])
                        elif self._item_type == "dict":
                            summary_data.extend([summary_item])

        return sorted(
            summary_data,
            key=lambda x: ((x[0], x[1]) if (self._item_type == "list") else (x["dataset"], x["core_id"])),
        )

    def get_detailed_data(self, excel=False) -> List[List]:
        detailed_data = []
        for validation_result in self._results:
            detailed_data = detailed_data + self._generate_error_details(validation_result, excel)
        return sorted(
            detailed_data,
            key=lambda x: ((x[0], x[3]) if (self._item_type == "list") else (x["core_id"], x["dataset"])),
        )

    def _generate_error_details(self, validation_result: RuleValidationResult, excel) -> List[List]:
        """
        Generates the Issue details data that goes into the excel export.
        Each row is represented by a list or a dict containing the following
        information:
        return [
            "CORE-ID",
            "Message",
            "Executability",
            "Dataset Name"
            "USUBJID",
            "Record",
            "Sequence",
            "Variable(s)",
            "Value(s)"
        ]
        """
        errors = []
        for result in validation_result.results or []:
            if result.get("errors", []) and result.get("executionStatus") == "success":
                variables = result.get("variables", [])
                for error in result.get("errors"):
                    error_item = {
                        "core_id": validation_result.id,
                        "message": result.get("message"),
                        "executability": validation_result.executability,
                        "dataset": error.get("dataset"),
                        "USUBJID": error.get("USUBJID", ""),
                        "row": error.get("row", ""),
                        "SEQ": error.get("SEQ", ""),
                    }
                    values = [str(error.get("value", {}).get(variable)) for variable in variables]
                    processed_values = self.process_values(values, excel)
                    if self._item_type == "list":
                        error_item["variables"] = ", ".join(variables)
                        error_item["values"] = processed_values
                        errors.append(list(error_item.values()))
                    elif self._item_type == "dict":
                        error_item["variables"] = variables
                        error_item["values"] = processed_values
                        errors.append(error_item)
        return errors

    def process_values(self, values: List[str], excel: bool) -> Union[str, List[str]]:
        if not values or values is None:
            return "null" if excel else ["null"]
        processed_values = []
        for value in values:
            value = value.strip()
            if value == "" or value.lower() == "none" or value.lower() == "nan":
                processed_values.append("null")
            else:
                processed_values.append(value)
        return ", ".join(processed_values) if excel else processed_values

    def get_rules_report_data(self) -> List[List]:
        """
        Generates the rules report data that goes into the excel export.
        Each row is represented by a list or a dict containing the following
        information:
        [
            "CORE-ID",
            "Version",
            "CDISC RuleID",
            "FDA RuleID",
            "Message",
            "Status",
            "Reason for Skip"
        ]
        """
        rules_report = []
        for validation_result in self._results:
            skipped = validation_result.execution_status != ExecutionStatus.SUCCESS.value
            rules_item = {
                "core_id": validation_result.id,
                "version": "1",
                "cdisc_rule_id": validation_result.cdisc_rule_id,
                "fda_rule_id": validation_result.fda_rule_id,
                "message": validation_result.message,
                "status": (ExecutionStatus.SKIPPED.value.upper() if skipped else ExecutionStatus.SUCCESS.value.upper()),
                "skip_reason": (self._get_skip_reason(validation_result) if skipped else ""),
            }
            if self._item_type == "list":
                rules_report.append([*rules_item.values()])
            elif self._item_type == "dict":
                rules_report.append(rules_item)

        return sorted(
            rules_report,
            key=lambda x: x[0] if (self._item_type == "list") else x["core_id"],
        )

    @staticmethod
    def _display_dataset_name(filename: Optional[str]) -> Optional[str]:
        if not filename:
            return filename
        return Path(filename).stem.upper()

    @staticmethod
    def _get_skip_reason(validation_result) -> str:
        datasets_by_reason = {}
        for result in validation_result.results or []:
            if not isinstance(result, dict):
                continue
            if result.get("executionStatus") == ExecutionStatus.SUCCESS.value:
                continue
            reason = result.get("message")
            if not reason:
                continue
            dataset = None
            match = _PER_DATASET_SKIP_REASON.match(reason)
            if match:
                reason, dataset = match["reason"], match["dataset"].upper()
            datasets = datasets_by_reason.setdefault(reason, [])
            if dataset and dataset not in datasets:
                datasets.append(dataset)
        return "; ".join(
            f"{reason} ({', '.join(datasets)})" if datasets else reason
            for reason, datasets in datasets_by_reason.items()
        )

    @property
    @abstractmethod
    def _file_format(self) -> str:
        pass

    @abstractmethod
    def get_export(
        self,
        define_version,
        cdiscCt,
        standard,
        version,
        dictionary_versions={},
        **kwargs,
    ) -> Union[dict, Workbook]:
        pass

    @abstractmethod
    def write_report(self, **kwargs):
        pass
