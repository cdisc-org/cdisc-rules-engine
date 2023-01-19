import json
from datetime import datetime
from typing import BinaryIO, List, Optional, Iterable

from cdisc_rules_engine.enums.report_types import ReportTypes
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.utilities.reporting_utilities import (
    get_define_version,
)
from .base_report import BaseReport


class JsonReport(BaseReport):
    """
    Generates a json report for a given set of validation results.
    """

    def __init__(
        self,
        dataset_paths: Iterable[str],
        validation_results: List[RuleValidationResult],
        elapsed_time: float,
        args: Validation_args,
        template: Optional[BinaryIO] = None,
    ):
        super().__init__(
            dataset_paths, validation_results, elapsed_time, args, template
        )
        self._item_type = "dict"

    @property
    def _file_format(self) -> str:
        return ReportTypes.JSON.value.lower()

    def get_export(self, define_version, cdiscCt, standard, version, **kwargs) -> dict:
        json_export = {
            "conformance_details": {
                "data_path": self._dataset_paths,
                "report_date": datetime.now().replace(microsecond=0).isoformat(),
                "runtime": round(self._elapsed_time, 2),
            },
            "bundle_details": {
                "standard": standard.upper(),
                "version": version,
                "cdisc_ct": cdiscCt,
                "define_version": define_version,
            },
        }
        if kwargs.get("raw_report") is True:
            json_export["results_data"] = [
                rule_result.to_representation() for rule_result in self._results
            ]
        else:
            json_export["summary_data"] = self.get_summary_data()
            json_export["rules_report_data"] = self.get_rules_report_data()
            json_export["detailed_data"] = self.get_detailed_data()

        return json_export

    def write_report(self):
        define_version: str = self._args.define_version or get_define_version(
            self._args.dataset_paths
        )
        report_data = self.get_export(
            define_version,
            list(self._args.controlled_terminology_package),
            self._args.standard,
            self._args.version.replace("-", "."),
            raw_report=self._args.raw_report,
        )
        with open(self._output_name, "w") as f:
            json.dump(report_data, f)
