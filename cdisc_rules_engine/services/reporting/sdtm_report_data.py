from datetime import datetime
from os import listdir
from os.path import dirname, join
from pathlib import Path
from typing import BinaryIO, Iterable
from cdisc_rules_engine.constants.define_xml_constants import DEFINE_XML_FILE_NAME
from cdisc_rules_engine.enums.default_file_paths import DefaultFilePaths
from cdisc_rules_engine.enums.execution_status import ExecutionStatus
from cdisc_rules_engine.models.dictionaries.dictionary_types import DictionaryTypes
from cdisc_rules_engine.services.define_xml.define_xml_reader_factory import (
    DefineXMLReaderFactory,
)
from cdisc_rules_engine.services.reporting.base_report_data import (
    BaseReportData,
)
from cdisc_rules_engine.services.reporting.report_metadata_item import (
    ReportMetadataItem,
)
from version import __version__
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata


class SDTMReportData(BaseReportData):
    """
    Report details specific to SDTM
    """

    TEMPLATE_FILE_PATH = DefaultFilePaths.EXCEL_TEMPLATE_FILE.value

    def __init__(
        self,
        datasets: Iterable[SDTMDatasetMetadata],
        dataset_paths: Iterable[str],
        validation_results: list[RuleValidationResult],
        elapsed_time: float,
        args: Validation_args,
        template: BinaryIO | None = None,
        **kwargs,
    ):
        super().__init__(
            datasets,
            dataset_paths,
            validation_results,
            elapsed_time,
            args,
            template,
            **kwargs,
        )
        define_xml_path = args.define_xml_path
        dictionary_versions = self._dictionary_versions or {}
        if define_xml_path:
            define_version = self.get_define_version([define_xml_path])
        else:
            define_version: str = self._args.define_version
        controlled_terminology = self._args.controlled_terminology_package
        if not controlled_terminology and define_version:
            if define_xml_path and define_version:
                controlled_terminology = self.get_define_ct(
                    [define_xml_path], define_version
                )
            else:
                controlled_terminology = self.get_define_ct(
                    self._args.dataset_paths, define_version
                )
        substandard = (
            self._args.substandard if hasattr(self._args, "substandard") else None
        )
        self.data_sheets = {
            "Conformance Details": self.get_conformance_details_data(
                define_version,
                controlled_terminology,
                dictionary_versions,
                substandard=substandard,
            ),
            "Dataset Details": self.get_dataset_details_data(),
            "Issue Summary": self.get_summary_data(),
            "Issue Details": self.get_detailed_data(),
            "Rules Report": self.get_rules_report_data(),
        }

    def get_conformance_details_data(
        self,
        define_version,
        cdiscCt,
        dictionary_versions,
        file_part: int = None,
        total_parts: int = None,
        **kwargs,
    ):
        timestamp = datetime.now().replace(microsecond=0).isoformat()
        if file_part and total_parts:
            timestamp += f" (Part {file_part} of {total_parts})"
        conformance_details = []
        conformance_details.append(
            ReportMetadataItem("Report Generation", 2, timestamp)
        )
        conformance_details.append(
            ReportMetadataItem(
                "Total Runtime", 3, f"{round(self._elapsed_time, 2)} seconds"
            )
        )
        conformance_details.append(
            ReportMetadataItem("CORE Engine Version", 4, __version__)
        )
        conformance_details.append(
            ReportMetadataItem(
                "Issue Limit Per Rule",
                5,
                "None" if self._max_errors_limit is None else self._max_errors_limit,
            )
        )
        conformance_details.append(
            ReportMetadataItem(
                "Issue Limit Per Dataset",
                6,
                "True" if self._errors_per_dataset_flag else "None",
            )
        )
        conformance_details.append(
            ReportMetadataItem(
                name="Issue Limit Per Sheet",
                row=7,
            )
        )
        conformance_details.append(ReportMetadataItem("Standard", 9, self._standard))
        if "substandard" in kwargs and kwargs["substandard"] is not None:
            conformance_details.append(
                ReportMetadataItem("Sub-Standard", 10, kwargs["substandard"])
            )
        conformance_details.append(
            ReportMetadataItem("Version", 11, f"V{self._version}")
        )
        if cdiscCt:
            conformance_details.append(
                ReportMetadataItem(
                    "CT Version",
                    12,
                    (
                        ", ".join(cdiscCt)
                        if isinstance(cdiscCt, (list, tuple, set))
                        else str(cdiscCt)
                    ),
                )
            )
        else:
            conformance_details.append(ReportMetadataItem("CT Version", 12, ""))
        conformance_details.append(
            ReportMetadataItem("Define-XML Version", 13, define_version)
        )

        # Populate external dictionary versions
        unii_version = dictionary_versions.get(DictionaryTypes.UNII.value)
        if unii_version is not None:
            conformance_details.append(
                ReportMetadataItem("UNII Version", 16, unii_version)
            )
        medrt_version = dictionary_versions.get(DictionaryTypes.MEDRT.value)
        if medrt_version is not None:
            conformance_details.append(
                ReportMetadataItem("Med-RT Version", 17, medrt_version)
            )
        meddra_version = dictionary_versions.get(DictionaryTypes.MEDDRA.value)
        if meddra_version is not None:
            conformance_details.append(
                ReportMetadataItem("MedDRA Version", 18, meddra_version)
            )
        whodrug_version = dictionary_versions.get(DictionaryTypes.WHODRUG.value)
        if whodrug_version is not None:
            conformance_details.append(
                ReportMetadataItem("WHODRUG Version", 19, whodrug_version)
            )
        snomed_version = dictionary_versions.get(DictionaryTypes.SNOMED.value)
        if snomed_version is not None:
            conformance_details.append(
                ReportMetadataItem("SNOMED Version", 20, snomed_version)
            )
        loinc_version = dictionary_versions.get(DictionaryTypes.LOINC.value)
        if loinc_version is not None:
            conformance_details.append(
                ReportMetadataItem("LOINC Version", 21, loinc_version)
            )
        return conformance_details

    def get_dataset_details_data(self) -> list[dict]:
        return [
            {
                "filename": dataset.name,
                "label": dataset.label,
                "path": str(Path(dataset.full_path or "").parent),
                "modification_date": dataset.modification_date,
                "size_kb": (dataset.file_size or 0) / 1000,
                "length": dataset.record_count,
            }
            for dataset in self._datasets
        ]

    def get_summary_data(self) -> list[dict]:
        """
        Generates the Issue Summary data that goes into the export.
        Each row is represented by a list or a dict containing the following
        information:
        return [
            "Dataset",
            "CORE-ID",
            "Message",
            "Issues",
            "Explanation"
        ]
        """
        summary_data = []
        for validation_result in self._results:
            if validation_result.execution_status == "success":
                for result in validation_result.results or []:
                    dataset = result.get("dataset")
                    if (
                        result.get("errors")
                        and result.get("executionStatus") == "success"
                    ):
                        summary_item = {
                            "dataset": dataset,
                            "core_id": validation_result.id,
                            "message": result.get("message"),
                            "issues": len(result.get("errors")),
                        }
                        summary_data.append(summary_item)

        return sorted(
            summary_data,
            key=lambda x: (x["dataset"], x["core_id"]),
        )

    def get_detailed_data(self, excel=False) -> list[dict]:
        detailed_data = []
        for validation_result in self._results:
            detailed_data = detailed_data + self._generate_error_details(
                validation_result, excel
            )
        return sorted(
            detailed_data,
            key=lambda x: (x["core_id"], x["dataset"]),
        )

    def _generate_error_details(
        self, validation_result: RuleValidationResult, excel
    ) -> list[dict]:
        """
        Generates the Issue details data that goes into the excel export.
        Each row is represented by a list or a dict containing the following
        information:
        return [
            "CORE-ID",
            "Message",
            "Executability",
            "Dataset Name"
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
                    values = []
                    for variable in variables:
                        raw_value = error.get("value", {}).get(variable)
                        if raw_value is None:
                            values.append(None)
                        else:
                            values.append(str(raw_value))
                    error_item = {
                        "core_id": validation_result.id,
                        "message": result.get("message"),
                        "executability": validation_result.executability,
                        "dataset": error.get("dataset"),
                        "USUBJID": error.get("USUBJID", ""),
                        "row": error.get("row", ""),
                        "SEQ": error.get("SEQ", ""),
                        "variables": variables,
                        "values": self.process_values(values),
                    }
                    errors.append(error_item)
        return errors

    def get_rules_report_data(self) -> list[dict]:
        """
        Generates the rules report data that goes into the excel export.
        Each row is represented by a list or a dict containing the following
        information:
        [
            "CORE-ID",
            "Version",
            "CDISC RuleID",
            "FDA RuleID",
            "Message",
            "Status"
        ]
        """
        rules_report = []
        for validation_result in self._results:
            rules_item = {
                "core_id": validation_result.id,
                "version": "1",
                "cdisc_rule_id": validation_result.cdisc_rule_id,
                "fda_rule_id": validation_result.fda_rule_id,
                "message": validation_result.message,
                "status": (
                    ExecutionStatus.SUCCESS.value.upper()
                    if validation_result.execution_status
                    == ExecutionStatus.SUCCESS.value
                    else ExecutionStatus.SKIPPED.value.upper()
                ),
            }
            rules_report.append(rules_item)
        return sorted(
            rules_report,
            key=lambda x: x["core_id"],
        )

    @staticmethod
    def get_define_version(dataset_paths: list[str]) -> str | None:
        """
        Method used to extract define version from xml file located
        in the same directory with datasets
        """
        if not dataset_paths:
            return None
        if dataset_paths[0].endswith(".xml"):
            define_xml_reader = DefineXMLReaderFactory.from_filename(dataset_paths[0])
            return define_xml_reader.get_define_version()
        path_to_data = dirname(dataset_paths[0])
        if not path_to_data or DEFINE_XML_FILE_NAME not in listdir(path_to_data):
            return None
        path_to_define = join(path_to_data, DEFINE_XML_FILE_NAME)
        define_xml_reader = DefineXMLReaderFactory.from_filename(path_to_define)
        version = define_xml_reader.get_define_version()
        return version

    @staticmethod
    def get_define_ct(dataset_paths: list[str], define_version) -> str | None:
        """
        Method used to extract CT version from define xml file located
        in the same directory with datasets
        """
        if not dataset_paths:
            return None
        ct = None
        if dataset_paths[0].endswith(".xml") and define_version == "2.1.0":
            define_xml_reader = DefineXMLReaderFactory.from_filename(dataset_paths[0])
            return define_xml_reader.get_ct_version()
        path_to_data = dirname(dataset_paths[0])
        if not path_to_data or DEFINE_XML_FILE_NAME not in listdir(path_to_data):
            return None
        path_to_define = join(path_to_data, DEFINE_XML_FILE_NAME)
        define_xml_reader = DefineXMLReaderFactory.from_filename(path_to_define)
        if define_version == "2.1.0":
            ct = define_xml_reader.get_ct_version()
        return ct
