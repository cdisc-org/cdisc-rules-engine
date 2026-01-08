from abc import ABC
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
    def process_values(values: list[str]) -> list[str]:
        if not values or values is None:
            return ["null"]
        processed_values = []
        for value in values:
            if value is None:
                processed_values.append("null")
                continue
            value = value.strip()
            if value == "" or value.lower() == "nan":
                processed_values.append("null")
            else:
                processed_values.append(value)
        return processed_values
