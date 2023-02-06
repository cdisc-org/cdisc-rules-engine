from abc import ABC, abstractmethod
from io import IOBase
from typing import List, Optional, Union, Iterable

from openpyxl import Workbook

from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.models.validation_args import Validation_args


class BaseReport(ABC):
    """
    Generates a base report for a given set of validation results.
    """

    def __init__(
        self,
        dataset_paths: Iterable[str],
        validation_results: List[RuleValidationResult],
        elapsed_time: float,
        args: Validation_args,
        template: Optional[IOBase] = None,
    ):
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
            "RuleID",
            "Message",
            "Executability",
            "Issues",
            "Explanation"
        ]
        """
        summary_data = []
        for validation_result in self._results:
            if validation_result.execution_status == "success":
                for result in validation_result.results or []:
                    dataset = result.get("domain")
                    if (
                        result.get("errors")
                        and result.get("executionStatus") == "success"
                    ):
                        summary_item = {
                            "dataset": dataset,
                            "rule_id": validation_result.id,
                            "message": result.get("message"),
                            "executability": validation_result.executability,
                            "issues": len(result.get("errors")),
                        }

                        if self._item_type == "list":
                            summary_data.extend([[*summary_item.values()]])
                        elif self._item_type == "dict":
                            summary_data.extend([summary_item])

        return sorted(
            summary_data,
            key=lambda x: (x[0], x[1])
            if (self._item_type == "list")
            else (x["dataset"], x["rule_id"]),
        )

    def get_detailed_data(self) -> List[List]:
        detailed_data = []
        for validation_result in self._results:
            detailed_data = detailed_data + self._generate_error_details(
                validation_result
            )
        return sorted(
            detailed_data,
            key=lambda x: (x[0], x[3])
            if (self._item_type == "list")
            else (x["rule_id"], x["dataset"]),
        )

    def _generate_error_details(self, validation_result) -> List[List]:
        """
        Generates the Issue details data that goes into the excel export.
        Each row is represented by a list or a dict containing the following
        information:
        return [
            "RuleID",
            "Message",
            "Executability",
            "Dataset",
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
                        "rule_id": validation_result.id,
                        "message": result.get("message"),
                        "executability": validation_result.executability,
                        "dataset": result.get("domain"),
                        "uSubjId": error.get("uSubjId", ""),
                        "row": error.get("row", ""),
                        "seq": error.get("seq", ""),
                    }

                    if self._item_type == "list":
                        error_item["variables"] = ", ".join(variables)
                        error_item["values"] = ", ".join(
                            [
                                str(error.get("value", {}).get(variable))
                                for variable in variables
                            ]
                        )
                        errors = errors + [[*error_item.values()]]
                    elif self._item_type == "dict":
                        error_item["variables"] = variables
                        error_item["values"] = [
                            str(error.get("value", {}).get(variable))
                            for variable in variables
                        ]
                        errors = errors + [error_item]
        return errors

    def get_rules_report_data(self) -> List[List]:
        """
        Generates the rules report data that goes into the excel export.
        Each row is represented by a list or a dict containing the following
        information:
        [
            "RuleID",
            "Version",
            "Message",
            "Status"
        ]
        """
        rules_report = []
        for validation_result in self._results:
            rules_item = {
                "rule_id": validation_result.id,
                "version": "1",
                "message": validation_result.message,
                "status": ExecutionStatus.SUCCESS.value.upper()
                if validation_result.execution_status == ExecutionStatus.SUCCESS.value
                else ExecutionStatus.SKIPPED.value.upper(),
            }
            if self._item_type == "list":
                rules_report.append([*rules_item.values()])
            elif self._item_type == "dict":
                rules_report.append(rules_item)

        return sorted(
            rules_report,
            key=lambda x: x[0] if (self._item_type == "list") else x["rule_id"],
        )

    @property
    @abstractmethod
    def _file_format(self) -> str:
        pass

    @abstractmethod
    def get_export(
        self, define_version, cdiscCt, standard, version, **kwargs
    ) -> Union[dict, Workbook]:
        pass

    @abstractmethod
    def write_report(self):
        pass
