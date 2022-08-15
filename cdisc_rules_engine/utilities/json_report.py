import json
from datetime import datetime
from typing import List
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.utilities.base_report import BaseReport
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult


class JsonReport(BaseReport):
    """
    Generates a json report for a given set of validation results.
    """

    def __init__(
        self,
        data_path: str,
        validation_results: List[RuleValidationResult],
        elapsed_time: float,
        args: Validation_args,
    ):
        super().__init__(data_path, validation_results, elapsed_time, args)
        self._item_type = "dict"

    def get_export(
        self, define_version, cdiscCt, standard, version, raw_report
    ) -> dict:
        json_export = {}
        json_export["conformance_details"] = {
            "data_path": self._data_path,
            "report_date": datetime.now().replace(microsecond=0).isoformat(),
            "runtime": round(self._elapsed_time, 2),
        }
        json_export["bundle_details"] = {
            "standard": standard.upper(),
            "version": version,
            "cdisc_ct": cdiscCt,
            "define_version": define_version,
        }
        if raw_report:
            json_export["results_data"] = [
                rule_result.to_representation() for rule_result in self._results
            ]
        else:
            json_export["summary_data"] = self.get_summary_data()
            json_export["rules_report_data"] = self.get_rules_report_data()
            json_export["detailed_data"] = self.get_detailed_data()

        return json_export

    def write_report(self):
        output_name = self._args.output + "." + self._args.output_format.lower()
        report_data = self.get_export(
            self._args.define_version,
            self._args.controlled_terminology_package,
            self._args.standard,
            self._args.version.replace("-", "."),
            self._args.raw_report,
        )
        with open(output_name, "w") as f:
            json.dump(report_data, f)
