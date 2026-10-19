import logging
from collections import Counter, defaultdict
from datetime import datetime
from typing import BinaryIO, List, Optional, Iterable

from openpyxl import Workbook

from version import __version__
from cdisc_rules_engine.enums.report_types import ReportTypes
from cdisc_rules_engine.models.rule_validation_result import RuleValidationResult
from cdisc_rules_engine.models.external_dictionaries_container import DictionaryTypes
from cdisc_rules_engine.models.validation_args import Validation_args
from .base_report import BaseReport
from .excel_writer import (
    excel_open_workbook,
    excel_update_worksheet,
    excel_workbook_to_stream,
    group_rows_by_key,
)
from cdisc_rules_engine.utilities.reporting_utilities import (
    get_define_version,
    get_define_ct,
)
from cdisc_rules_engine.utilities.report_progress import (
    ReportProgressReporter,
    DisabledReportProgressReporter,
)
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from pathlib import Path


def _collapse_key(row: List) -> tuple:
    # CORE-ID, Message, Executability, Dataset, Variable(s), Value(s)
    return (row[0], row[1], row[2], row[3], row[7], row[8])


def _format_row_range(values: List) -> str:
    numeric = []
    non_numeric = []
    for value in values:
        try:
            numeric.append(int(value))
        except (TypeError, ValueError):
            if value not in ("", None):
                non_numeric.append(str(value))
    numeric = sorted(set(numeric))
    ranges = []
    start = prev = None
    for n in numeric:
        if start is None:
            start = prev = n
        elif n == prev + 1:
            prev = n
        else:
            ranges.append(f"{start}-{prev}" if start != prev else str(start))
            start = prev = n
    if start is not None:
        ranges.append(f"{start}-{prev}" if start != prev else str(start))
    parts = ranges + sorted(set(non_numeric))
    max_parts = 25
    if len(parts) > max_parts:
        return ", ".join(parts[:max_parts]) + f", ... (+{len(parts) - max_parts} more)"
    return ", ".join(parts)


class ExcelReport(BaseReport):
    """
    Generates an excel report for a given set of validation results.
    """

    _EXCEL_MAX_DATA_ROWS = 1_048_576 - 1

    def __init__(
        self,
        datasets: Iterable[SDTMDatasetMetadata],
        dataset_paths: Iterable[str],
        validation_results: List[RuleValidationResult],
        elapsed_time: float,
        args: Validation_args,
        template: Optional[BinaryIO] = None,
    ):
        super().__init__(datasets, dataset_paths, validation_results, elapsed_time, args, template)
        self._item_type = "list"

    @property
    def _file_format(self):
        return ReportTypes.XLSX.value.lower()

    def get_export(self, define_version, cdiscCt, standard, version, dictionary_versions, **kwargs) -> Workbook:
        progress_reporter: ReportProgressReporter = kwargs.get("progress_reporter") or DisabledReportProgressReporter()
        wb = excel_open_workbook(self._template.read())
        detail_chunks = kwargs.get("detail_chunks")
        if detail_chunks is None:
            detail_chunks = self._get_issue_detail_chunks()

        progress_reporter.start_step("Writing Issue Summary")
        summary_data = self.get_summary_data()
        for row in summary_data:
            row[0] = self._display_dataset_name(row[0])
        excel_update_worksheet(wb["Issue Summary"], summary_data, dict(wrap_text=True))
        progress_reporter.end_step()

        self._write_issue_details(wb, detail_chunks, progress_reporter)

        progress_reporter.start_step("Writing Rules Report")
        rules_report_data = self.get_rules_report_data()
        excel_update_worksheet(wb["Rules Report"], rules_report_data, dict(wrap_text=True))
        progress_reporter.end_step()

        progress_reporter.start_step("Writing Dataset Details")
        # write conformance data
        wb["Conformance Details"]["B2"] = datetime.now().replace(microsecond=0).isoformat()
        wb["Conformance Details"]["B3"] = f"{round(self._elapsed_time, 2)} seconds"
        wb["Conformance Details"]["B4"] = __version__

        # write dataset metadata
        datasets_data = [
            [
                self._display_dataset_name(dataset.filename),
                dataset.label,
                str(Path(getattr(dataset, "full_path", "")).parent),
                getattr(dataset, "modification_date", ""),
                round((getattr(dataset, "file_size", None) or 0) / 1000, 1),
                getattr(dataset, "record_count", ""),
            ]
            for dataset in self._datasets
        ]
        excel_update_worksheet(wb["Dataset Details"], datasets_data, dict(wrap_text=True))

        # write standards details
        wb["Conformance Details"]["B7"] = standard.upper()
        if "substandard" in kwargs and kwargs["substandard"] is not None:
            wb["Conformance Details"]["B8"] = kwargs["substandard"]
        wb["Conformance Details"]["B9"] = f"V{version}"
        if cdiscCt:
            wb["Conformance Details"]["B10"] = (
                ", ".join(cdiscCt) if isinstance(cdiscCt, (list, tuple, set)) else str(cdiscCt)
            )
        else:
            wb["Conformance Details"]["B10"] = ""
        wb["Conformance Details"]["B11"] = define_version

        # Populate external dictionary versions
        unii_version = dictionary_versions.get(DictionaryTypes.UNII.value)
        if unii_version is not None:
            wb["Conformance Details"]["B12"] = unii_version

        medrt_version = dictionary_versions.get(DictionaryTypes.MEDRT.value)
        if medrt_version is not None:
            wb["Conformance Details"]["B13"] = medrt_version

        meddra_version = dictionary_versions.get(DictionaryTypes.MEDDRA.value)
        if meddra_version is not None:
            wb["Conformance Details"]["B14"] = meddra_version

        whodrug_version = dictionary_versions.get(DictionaryTypes.WHODRUG.value)
        if whodrug_version is not None:
            wb["Conformance Details"]["B15"] = whodrug_version

        snomed_version = dictionary_versions.get(DictionaryTypes.SNOMED.value)
        if snomed_version is not None:
            wb["Conformance Details"]["B16"] = snomed_version
        progress_reporter.end_step()
        return wb

    def _get_issue_detail_chunks(self) -> List[List[List]]:
        detailed_data = self.get_detailed_data(excel=True)
        if getattr(self._args, "collapse_duplicate_issues", False):
            detailed_data = self._collapse_duplicate_detail_rows(detailed_data)
        max_rows = self._EXCEL_MAX_DATA_ROWS
        return [detailed_data[i : i + max_rows] for i in range(0, max(len(detailed_data), 1), max_rows)]

    def _write_issue_details(
        self,
        wb: Workbook,
        chunks: List[List[List]],
        progress_reporter: ReportProgressReporter,
    ) -> None:
        """
        Write each chunk to its own sheet.
        "Issue Details" for the first, "Issue Details N" copies for overflow.
        """
        base_sheet = wb["Issue Details"]
        sheets = [base_sheet]
        for index in range(1, len(chunks)):
            overflow_sheet = wb.copy_worksheet(base_sheet)
            overflow_sheet.title = f"Issue Details {index + 1}"
            sheets.append(overflow_sheet)
        for index, (sheet, chunk) in enumerate(zip(sheets, chunks), start=1):
            label = "Writing Issue Details"
            if len(chunks) > 1:
                label += f" (sheet {index}/{len(chunks)})"
            progress_reporter.start_step(label)
            for row in chunk:
                row[3] = self._display_dataset_name(row[3])
            excel_update_worksheet(sheet, chunk, dict(wrap_text=True))
            group_rows_by_key(sheet, chunk, key_col=0)
            progress_reporter.end_step()

    def _get_dataset_record_counts(self) -> dict:
        counts = {}
        for dataset in self._datasets:
            record_count = getattr(dataset, "record_count", None)
            if record_count is None:
                continue
            try:
                counts[dataset.filename] = int(record_count)
            except (TypeError, ValueError):
                pass
        return counts

    def _collapse_duplicate_detail_rows(self, detailed_data: List[List]) -> List[List]:
        """
        When a message applies to every record in a dataset we collapse those rows into
        one row with "row"/"SEQ" as a compact range.
        USUBJID is excluded from the grouping key since it isn't meaningful once collapsed.
        """
        dataset_record_counts = self._get_dataset_record_counts()
        counts = Counter(_collapse_key(row) for row in detailed_data)
        collapsible = {key for key, count in counts.items() if dataset_record_counts.get(key[3]) == count}
        if not collapsible:
            return detailed_data

        members = defaultdict(list)
        for row in detailed_data:
            key = _collapse_key(row)
            if key in collapsible:
                members[key].append(row)

        collapsed = []
        emitted = set()
        for row in detailed_data:
            key = _collapse_key(row)
            if key not in collapsible:
                collapsed.append(row)
            elif key not in emitted:
                emitted.add(key)
                entries = members[key]
                summary = list(entries[0])
                summary[4] = ""
                summary[5] = _format_row_range([entry[5] for entry in entries])
                summary[6] = _format_row_range([entry[6] for entry in entries])
                collapsed.append(summary)
        return collapsed

    def write_report(self, **kwargs):
        logger = logging.getLogger("validator")
        define_xml_path = kwargs.get("define_xml_path")
        dictionary_versions = kwargs.get("dictionary_versions", {})
        progress_reporter: ReportProgressReporter = kwargs.get("progress_reporter") or DisabledReportProgressReporter()
        try:
            if define_xml_path:
                define_version = get_define_version([define_xml_path])
            else:
                define_version: str = self._args.define_version
            controlled_terminology = self._args.controlled_terminology_package
            if not controlled_terminology and define_version:
                if define_xml_path and define_version:
                    controlled_terminology = get_define_ct([define_xml_path], define_version)
                else:
                    controlled_terminology = get_define_ct(self._args.dataset_paths, define_version)

            detail_chunks = self._get_issue_detail_chunks()
            total_steps = len(detail_chunks) + 4
            progress_reporter.start(total_steps)

            report_data = self.get_export(
                define_version,
                controlled_terminology,
                self._args.standard,
                self._args.version.replace("-", "."),
                dictionary_versions,
                substandard=(self._args.substandard if hasattr(self._args, "substandard") else None),
                progress_reporter=progress_reporter,
                detail_chunks=detail_chunks,
            )

            progress_reporter.start_step("Saving workbook to disk")
            with open(self._output_name, "wb") as f:
                f.write(excel_workbook_to_stream(report_data))
            progress_reporter.end_step()
        except Exception as e:
            logger.error(e)
            raise e
        finally:
            self._template.close()
            progress_reporter.finish()
