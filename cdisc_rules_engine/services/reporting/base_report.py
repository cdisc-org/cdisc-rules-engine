from abc import ABC, abstractmethod
from io import IOBase

from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.services.reporting.base_report_data import (
    BaseReportData,
)


class BaseReport(ABC):
    """
    Generates a base report for a given set of validation results.
    """

    def __init__(
        self,
        report_standard: BaseReportData,
        args: Validation_args,
        template: IOBase | None = None,
    ):
        self._report_standard = report_standard
        self._args = args
        self._template = template
        self._output_name: str = self._get_output_filename()

    def _get_output_filename(self) -> str:
        expected_ext = f".{self._file_ext}"
        output_path = self._args.output

        if output_path.lower().endswith(expected_ext.lower()):
            base_path = output_path[: -len(expected_ext)]
            return f"{base_path}{expected_ext}"

        path_lower = output_path.lower()
        known_extensions = [".json", ".xlsx", ".xls"]
        for ext in known_extensions:
            if path_lower.endswith(ext):
                base_path = output_path[: -len(ext)]
                return f"{base_path}{expected_ext}"

        return f"{output_path}{expected_ext}"

    @property
    @abstractmethod
    def _file_ext(self) -> str:
        pass

    @abstractmethod
    def write_report(self):
        pass
