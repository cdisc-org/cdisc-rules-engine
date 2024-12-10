import json
from datetime import datetime
from typing import BinaryIO, List, Optional, Iterable

from cdisc_rules_engine.enums.report_types import ReportTypes
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.utilities.reporting_utilities import (
    get_define_version,
    get_define_ct,
)
from .base_report import BaseReport
from version import __version__
from pathlib import Path
from cdisc_rules_engine.models.external_dictionaries_container import DictionaryTypes


class JsonReport(BaseReport):
    """
    Generates a json report for a given set of validation results.
    """

    def __init__(
        self,
        datasets: Iterable[dict],
        dataset_paths: Iterable[str],
        validation_results: List[RuleValidationResult],
        elapsed_time: float,
        args: Validation_args,
        template: Optional[BinaryIO] = None,
    ):
        super().__init__(
            datasets, dataset_paths, validation_results, elapsed_time, args, template
        )
        self._item_type = "dict"

    @property
    def _file_format(self) -> str:
        return ReportTypes.JSON.value.lower()

    def get_export(
        self,
        define_version,
        cdiscCt,
        standard,
        version,
        dictionary_versions={},
        **kwargs,
    ) -> dict:
        conformance_details = {
            "CORE_Engine_Version": __version__,
            "Report_Generation": datetime.now().replace(microsecond=0).isoformat(),
            "Total_Runtime": f"{round(self._elapsed_time, 2)} seconds",
            "Standard": standard.upper(),
            "Version": f"V{version}",
            "CT_Version": ", ".join(cdiscCt),
            "Define_XML_Version": define_version,
        }
        conformance_details["UNII_Version"] = dictionary_versions.get(
            DictionaryTypes.UNII.value
        )
        conformance_details["Med-RT_Version"] = dictionary_versions.get(
            DictionaryTypes.MEDRT.value
        )
        conformance_details["Meddra_Version"] = dictionary_versions.get(
            DictionaryTypes.MEDDRA.value
        )
        conformance_details["WHODRUG_Version"] = dictionary_versions.get(
            DictionaryTypes.WHODRUG.value
        )
        conformance_details["LOINC_Version"] = dictionary_versions.get(
            DictionaryTypes.LOINC.value
        )
        conformance_details["SNOMED_Version"] = None

        json_export = {
            "Conformance_Details": conformance_details,
            "Dataset_Details": [
                {
                    "filename": dataset.get("filename"),
                    "label": dataset.get("label"),
                    "path": str(Path(dataset.get("full_path", "")).parent),
                    "modification_date": dataset.get("modification_date"),
                    "size_kb": dataset.get("size", 0) / 1000,
                    "length": dataset.get("length"),
                }
                for dataset in self._datasets
            ],
        }

        if kwargs.get("raw_report") is True:
            json_export["results_data"] = [
                rule_result.to_representation() for rule_result in self._results
            ]
        else:
            json_export["Issue_Summary"] = self.get_summary_data()
            json_export["Issue_Details"] = self.get_detailed_data()
            json_export["Rules_Report"] = self.get_rules_report_data()
        return json_export

    def write_report(self, **kwargs):
        define_xml_path = kwargs.get("define_xml_path")
        dictionary_versions = kwargs.get("dictionary_versions", {})
        if define_xml_path:
            define_version = get_define_version([define_xml_path])
        else:
            define_version: str = self._args.define_version or get_define_version(
                self._args.dataset_paths
            )
        controlled_terminology = self._args.controlled_terminology_package
        if not controlled_terminology and define_version:
            if define_xml_path and define_version:
                controlled_terminology = get_define_ct(
                    [define_xml_path], define_version
                )
            else:
                controlled_terminology = get_define_ct(
                    self._args.dataset_paths, define_version
                )
        report_data = self.get_export(
            define_version,
            list(controlled_terminology),
            self._args.standard,
            self._args.version.replace("-", "."),
            raw_report=self._args.raw_report,
            dictionary_versions=dictionary_versions,
        )
        with open(self._output_name, "w") as f:
            json.dump(report_data, f)
