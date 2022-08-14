from datetime import datetime
from typing import List, TextIO
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.utilities.generic_report import GenericReport


class JsonReport(GenericReport):
    """
    Generates a json report for a given set of validation results.
    """

    def get_json_export(self, define_version, cdiscCt, standard, version, raw_report) -> dict:
        """
        Populates the excel workbook template found in the file "CORE-Report-Template.xlsx" with
        data from validation results
        """

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
        if (raw_report):
            results_data = []
            for rule_result in self._results:
                results_data.append(rule_result.to_representation())
            json_export["results_data"] = results_data;
        else:
            json_export["summary_data"] = self.get_summary_data()
            json_export["rules_report_data"] = self.get_rules_report_data()
            json_export["detailed_data"] = self.get_detailed_data()

        return json_export
