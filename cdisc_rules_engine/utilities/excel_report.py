from datetime import datetime
from typing import List, TextIO

from openpyxl import Workbook

from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.utilities.excel_writer import (
    excel_open_workbook,
    excel_update_worksheet,
)


class ExcelReport:
    """
    Generates an excel report for a given set of validation results.
    """

    def __init__(
        self,
        data_path: str,
        validation_results: List[RuleValidationResult],
        elapsed_time: float,
        report_template: TextIO,
    ):
        self._data_path: str = data_path
        self._elapsed_time: int = elapsed_time
        self._results: List[RuleValidationResult] = validation_results
        self._template: TextIO = report_template

    def get_summary_data(self) -> List[List]:
        """
        Generates the Issue Summary data that goes into the excel export.
        Each row is represented by a list containing the following information:
        return [
            "Dataset",
            "RuleID",
            "Message",
            "Severity",
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
                        summary_data.extend(
                            [
                                [
                                    dataset,
                                    validation_result.id,  # rule id
                                    result.get("message"),
                                    validation_result.severity,
                                    len(result.get("errors")),
                                ]
                            ]
                        )
        return sorted(summary_data, key=lambda x: (x[0], x[1]))

    def get_detailed_data(self) -> List[List]:
        detailed_data = []
        for validation_result in self._results:
            detailed_data = detailed_data + self._generate_error_details(
                validation_result
            )
        return sorted(detailed_data, key=lambda x: (x[0], x[3]))

    def _generate_error_details(self, validation_result) -> List[List]:
        """
        Generates the Issue details data that goes into the excel export.
        Each row is represented by a list containing the following information:
        return [
            "RuleID",
            "Message",
            "Severity",
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
                errors = errors + [
                    [
                        validation_result.id,
                        result.get("message"),
                        validation_result.severity,
                        result.get("domain"),
                        error.get("uSubjId", ""),
                        error.get("row", ""),
                        error.get("seq", ""),
                        ", ".join(variables),
                        ", ".join(
                            [
                                str(error.get("value", {}).get(variable))
                                for variable in variables
                            ]
                        ),
                    ]
                    for error in result.get("errors")
                ]
        return errors

    def get_rules_report_data(self) -> List[List]:
        """
        Generates the rules report data that goes into the excel export.
        Each row is represented by a list containing the following information:
        [
            "RuleID",
            "Version",
            "Message",
            "Status"
        ]
        """
        rules_report = []
        for validation_result in self._results:
            rules_report.append(
                [
                    validation_result.id,
                    "1",
                    validation_result.message,
                    "SUCCESS"
                    if validation_result.execution_status == "success"
                    else "SKIPPED",
                ]
            )
        return sorted(rules_report, key=lambda x: x[0])

    def get_excel_export(self, define_version, cdiscCt, standard, version) -> Workbook:
        """
        Populates the excel workbook template found in the file "CORE-Report-Template.xlsx" with
        data from validation results
        """
        wb = excel_open_workbook(self._template)
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
