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
        self._output_name: str = f"{self._args.output}.{self._file_ext}"

    @property
    @abstractmethod
    def _file_ext(self) -> str:
        pass

    @abstractmethod
    def write_report(self):
        pass
