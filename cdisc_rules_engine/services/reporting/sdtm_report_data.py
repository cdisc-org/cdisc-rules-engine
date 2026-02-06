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


def _normalize_to_list(value):
    """Normalize various data structures to lists."""
    if isinstance(value, (list, tuple, set)):
        return list(value)
    if isinstance(value, dict):
        return list(value.keys())
    return [value] if value is not None else []


def _flatten_to_comparable(value):
    """Flatten nested structures to comparable primitives."""
    result = []
    for item in value:
        if isinstance(item, (list, tuple, set)):
            result.extend(_flatten_to_comparable(list(item)))
        elif isinstance(item, dict):
            result.extend(_flatten_to_comparable(list(item.keys())))
        else:
            result.append(item)
    return result


def _compare_data_structures(value1, value2):
    """Compare two data structures and return differences (set-based)."""
    if value1 is None and value2 is None:
        return {"missing_in_value2": [], "extra_in_value2": [], "common": []}
    if value1 is None or value2 is None:
        return {
            "missing_in_value2": (
                _normalize_to_list(value1) if value1 is not None else []
            ),
            "extra_in_value2": _normalize_to_list(value2) if value2 is not None else [],
            "common": [],
        }

    set1 = set(_flatten_to_comparable(_normalize_to_list(value1)))
    set2 = set(_flatten_to_comparable(_normalize_to_list(value2)))

    return {
        "missing_in_value2": sorted(set1 - set2),
        "extra_in_value2": sorted(set2 - set1),
        "common": sorted(set1 & set2),
    }


def _format_comparison_result(comparison, var1_name, var2_name):
    """Format comparison results as human-readable string."""
    missing = comparison.get("missing_in_value2", [])
    extra = comparison.get("extra_in_value2", [])

    parts = []
    if missing:
        parts.append(f"Missing in {var2_name}: {', '.join(map(str, missing))}")
    if extra:
        parts.append(f"Extra in {var2_name}: {', '.join(map(str, extra))}")
    if not parts:
        parts.append("No differences found")

    return "\n".join(parts)


class SDTMReportData(BaseReportData):
    """
    Report details specific to SDTM
    """

    TEMPLATE_FILE_PATH = DefaultFilePaths.EXCEL_TEMPLATE_FILE.value

    _SPECIAL_ERROR_FIELDS = {
        "USUBJID",
        "dataset",
        "row",
        "SEQ",
        "entity",
        "instance_id",
        "path",
    }

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
        use_case = self._args.use_case if hasattr(self._args, "use_case") else None
        self.data_sheets = {
            "Conformance Details": self.get_conformance_details_data(
                define_version,
                controlled_terminology,
                dictionary_versions,
                substandard=substandard,
                use_case=use_case,
            ),
            "Dataset Details": self.get_dataset_details_data(),
            "Issue Summary": self.get_summary_data(),
            "Issue Details": self.get_detailed_data(),
            "Rules Report": self.get_rules_report_data(),
        }

    def get_conformance_details_data(  # noqa
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
        if "use_case" in kwargs and kwargs["use_case"] is not None:
            conformance_details.append(
                ReportMetadataItem("TIG Use Case", 12, kwargs["use_case"])
            )
        if cdiscCt:
            conformance_details.append(
                ReportMetadataItem(
                    "CT Version",
                    13,
                    (
                        ", ".join(cdiscCt)
                        if isinstance(cdiscCt, (list, tuple, set))
                        else str(cdiscCt)
                    ),
                )
            )
        else:
            conformance_details.append(ReportMetadataItem("CT Version", 13, ""))
        conformance_details.append(
            ReportMetadataItem("Define-XML Version", 14, define_version)
        )

        # Populate external dictionary versions
        unii_version = dictionary_versions.get(DictionaryTypes.UNII.value)
        if unii_version is not None:
            conformance_details.append(
                ReportMetadataItem("UNII Version", 15, unii_version)
            )
        medrt_version = dictionary_versions.get(DictionaryTypes.MEDRT.value)
        if medrt_version is not None:
            conformance_details.append(
                ReportMetadataItem("Med-RT Version", 16, medrt_version)
            )
        meddra_version = dictionary_versions.get(DictionaryTypes.MEDDRA.value)
        if meddra_version is not None:
            conformance_details.append(
                ReportMetadataItem("MedDRA Version", 17, meddra_version)
            )
        whodrug_version = dictionary_versions.get(DictionaryTypes.WHODRUG.value)
        if whodrug_version is not None:
            conformance_details.append(
                ReportMetadataItem("WHODRUG Version", 18, whodrug_version)
            )
        snomed_version = dictionary_versions.get(DictionaryTypes.SNOMED.value)
        if snomed_version is not None:
            conformance_details.append(
                ReportMetadataItem("SNOMED Version", 19, snomed_version)
            )
        loinc_version = dictionary_versions.get(DictionaryTypes.LOINC.value)
        if loinc_version is not None:
            conformance_details.append(
                ReportMetadataItem("LOINC Version", 20, loinc_version)
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

    def _process_comparison_group(self, group: list, error_value: dict) -> str:
        """Process a single comparison group and return formatted comparison string."""
        if len(group) < 2:
            return ""

        baseline_name, baseline_value = group[0], error_value.get(group[0])

        summary_lines = [
            (
                _format_comparison_result(
                    _compare_data_structures(baseline_value, other_value),
                    baseline_name,
                    other_name,
                )
                if baseline_value is not None and other_value is not None
                else f"{other_name}: null vs {baseline_name}: null"
            )
            for other_name in group[1:]
            for other_value in [error_value.get(other_name)]
        ]

        raw_value_lines = [
            (
                f"{name}: {val}"
                if (val := error_value.get(name)) is not None
                else f"{name}: null"
            )
            for name in group
        ]

        return "\n".join(summary_lines + raw_value_lines)

    def _extract_value_for_variable(
        self, variable: str, error: dict, error_value: dict
    ) -> str | None:
        """Extract value for a variable, checking special fields first."""
        if variable in self._SPECIAL_ERROR_FIELDS:
            val = error.get(variable)
            if val is None:
                val = error_value.get(variable)
        else:
            val = error_value.get(variable)
        return None if val is None else str(val)

    def _extract_values_from_error(
        self, error: dict, error_value: dict, compare_groups: list, variables: list
    ) -> list:
        """Extract values from error, handling comparison groups or standard variables."""
        if not compare_groups:
            return [
                self._extract_value_for_variable(v, error, error_value)
                for v in variables
            ]

        compare_group_vars = {var for group in compare_groups for var in group}
        var_to_group = {var: group for group in compare_groups for var in group}
        processed_groups = set()

        result = []
        for variable in variables:
            if variable in compare_group_vars:
                group = var_to_group[variable]
                group_key = tuple(sorted(group))
                if group_key not in processed_groups:
                    result.append(self._process_comparison_group(group, error_value))
                    processed_groups.add(group_key)
            else:
                result.append(
                    self._extract_value_for_variable(variable, error, error_value)
                )

        return result

    def _create_error_item(
        self,
        validation_result: RuleValidationResult,
        result: dict,
        error: dict,
        variables: list,
        values: list,
    ) -> dict:
        """Create a single error item dictionary."""
        return {
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
                compare_groups = result.get("compare_groups") or []

                for error in result.get("errors"):
                    error_value = error.get("value", {})
                    values = self._extract_values_from_error(
                        error, error_value, compare_groups, variables
                    )
                    if compare_groups:
                        compare_group_vars = {
                            var for group in compare_groups for var in group
                        }
                        var_to_group = {
                            var: group for group in compare_groups for var in group
                        }
                        processed_groups = set()
                        aligned_variables = []
                        for variable in variables:
                            if variable in compare_group_vars:
                                group = var_to_group[variable]
                                group_key = tuple(sorted(group))
                                if group_key not in processed_groups:
                                    aligned_variables.append(", ".join(group))
                                    processed_groups.add(group_key)
                            else:
                                aligned_variables.append(variable)
                    else:
                        aligned_variables = variables
                    error_item = self._create_error_item(
                        validation_result, result, error, aligned_variables, values
                    )
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
