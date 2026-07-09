from abc import ABC, abstractmethod
from io import IOBase
from typing import Iterable

from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.services.reporting.report_metadata_item import (
    ReportMetadataItem,
)
from cdisc_rules_engine.utilities.utils import set_max_errors_per_rule


class BaseReportData(ABC):
    TEMPLATE_FILE_PATH: str
    data_sheets: dict[str, list[ReportMetadataItem | dict[str, str]]]

    def __init__(
        self,
        datasets: Iterable[SDTMDatasetMetadata],
        dataset_paths: Iterable[str],
        validation_results: list[RuleValidationResult],
        elapsed_time: float,
        args: Validation_args,
        template: IOBase | None = None,
        **kwargs,
    ):
        self._datasets = datasets
        self._dataset_paths: Iterable[str] = dataset_paths
        self._elapsed_time: float = elapsed_time
        self._results: list[RuleValidationResult] = validation_results
        self._args = args
        self._dictionary_versions = kwargs.get("dictionary_versions")
        self._template = template
        self._standard = args.standard.upper()
        self._version = args.version.replace("-", ".")
        self._max_errors_limit, self._errors_per_dataset_flag = set_max_errors_per_rule(
            args
        )

    @staticmethod
    def process_values(
        values: list[str | None], null_placeholder: str = "null"
    ) -> list[str]:
        if not values:
            return [null_placeholder]
        processed_values = []
        for value in values:
            if value is None:
                processed_values.append(null_placeholder)
                continue
            stripped = value.strip()
            if stripped == "" or stripped.lower() == "nan":
                processed_values.append(null_placeholder)
            else:
                processed_values.append(value)
        return processed_values

    @staticmethod
    def _csv_sort_key(row: list[str]) -> tuple:
        """
        Sorts numeric fields numerically rather than lexicographically.

        Each field becomes (0, int_value) for pure integers or (1, str_value)
        for everything else, so numbers sort before strings and 2 < 10.
        """
        parts = []
        for field in row:
            try:
                parts.append((0, int(field), ""))
            except ValueError:
                parts.append((1, 0, field))
        return tuple(parts)

    def get_csv_rows(self) -> tuple[list[str], list[list[str]]]:
        """
        Return (header, sorted_rows) for the CSV output format.
        Each row is a list of string values matching the header columns.
        Sorting is applied by full column order.
        """
        header, rows = self._get_csv_rows()
        return header, sorted(rows, key=self._csv_sort_key)

    @abstractmethod
    def _get_csv_rows(self) -> tuple[list[str], list[list[str]]]:
        """Return (header, rows) before base-class sorting is applied."""
        pass
