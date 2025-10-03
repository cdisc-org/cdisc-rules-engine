import logging
from datetime import datetime
from typing import BinaryIO, List, Optional, Iterable
import os

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
)
from cdisc_rules_engine.utilities.reporting_utilities import (
    get_define_version,
    get_define_ct,
)
from cdisc_rules_engine.models.sdtm_dataset_metadata import SDTMDatasetMetadata
from pathlib import Path


class ExcelReport(BaseReport):
    """
    Generates an excel report for a given set of validation results.
    """

    DEFAULT_MAX_ROWS = 1000000

    def __init__(
        self,
        datasets: Iterable[SDTMDatasetMetadata],
        dataset_paths: Iterable[str],
        validation_results: List[RuleValidationResult],
        elapsed_time: float,
        args: Validation_args,
        template: Optional[BinaryIO] = None,
    ):
        super().__init__(
            datasets, dataset_paths, validation_results, elapsed_time, args, template
        )
        self._item_type = "list"
        max_rows_options = []
        env_max_rows = os.getenv("MAX_ROWS_PER_SHEET")
        if env_max_rows:
            max_rows_options.append(int(env_max_rows))
        if args.max_report_rows is not None:
            max_rows_options.append(int(self._args.max_report_rows))
        if max_rows_options:
            self.max_rows_per_sheet = min(max_rows_options)
        else:
            self.max_rows_per_sheet = self.DEFAULT_MAX_ROWS

    @property
    def _file_format(self):
        return ReportTypes.XLSX.value.lower()

    def _chunk_data(self, data: List[List], chunk_size: int) -> List[List[List]]:
        chunks = []
        for i in range(0, len(data), chunk_size):
            chunks.append(data[i : i + chunk_size])
        return chunks

    def _needs_splitting(self, summary_data: List, detailed_data: List) -> bool:
        return (
            len(summary_data) > self.max_rows_per_sheet
            or len(detailed_data) > self.max_rows_per_sheet
        )

    def get_export(
        self,
        define_version,
        cdiscCt,
        standard,
        version,
        dictionary_versions,
        template_buffer,
        **kwargs,
    ) -> List[Workbook]:
        summary_data = self.get_summary_data()
        detailed_data = self.get_detailed_data(excel=True)
        rules_report_data = self.get_rules_report_data()
        if not self._needs_splitting(summary_data, detailed_data):
            # Single file - original behavior
            wb = self._create_single_workbook(
                template_buffer,
                summary_data,
                detailed_data,
                rules_report_data,
                define_version,
                cdiscCt,
                standard,
                version,
                dictionary_versions,
                **kwargs,
            )
            return [wb]
        else:
            # Multiple files needed
            return self._create_multiple_workbooks(
                template_buffer,
                summary_data,
                detailed_data,
                rules_report_data,
                define_version,
                cdiscCt,
                standard,
                version,
                dictionary_versions,
                **kwargs,
            )

    def _create_single_workbook(
        self,
        template_buffer,
        summary_data,
        detailed_data,
        rules_report_data,
        define_version,
        cdiscCt,
        standard,
        version,
        dictionary_versions,
        **kwargs,
    ) -> Workbook:
        wb = excel_open_workbook(template_buffer)
        excel_update_worksheet(wb["Issue Summary"], summary_data, dict(wrap_text=True))
        excel_update_worksheet(wb["Issue Details"], detailed_data, dict(wrap_text=True))
        excel_update_worksheet(
            wb["Rules Report"], rules_report_data, dict(wrap_text=True)
        )
        self._populate_metadata_sheets(
            wb,
            define_version,
            cdiscCt,
            standard,
            version,
            dictionary_versions,
            **kwargs,
        )
        return wb

    def _create_multiple_workbooks(
        self,
        template_buffer,
        summary_data,
        detailed_data,
        rules_report_data,
        define_version,
        cdiscCt,
        standard,
        version,
        dictionary_versions,
        **kwargs,
    ) -> List[Workbook]:
        """
        Create multiple workbooks when data exceeds Excel's row limit.
        """
        workbooks = []
        detailed_chunks = self._chunk_data(detailed_data, self.max_rows_per_sheet)
        summary_chunks = self._chunk_data(summary_data, self.max_rows_per_sheet)
        num_files = max(len(detailed_chunks), len(summary_chunks))
        for i in range(num_files):
            wb = excel_open_workbook(template_buffer)
            summary_chunk = summary_chunks[i] if i < len(summary_chunks) else []
            detailed_chunk = detailed_chunks[i] if i < len(detailed_chunks) else []
            excel_update_worksheet(
                wb["Issue Summary"], summary_chunk, dict(wrap_text=True)
            )
            excel_update_worksheet(
                wb["Issue Details"], detailed_chunk, dict(wrap_text=True)
            )
            excel_update_worksheet(
                wb["Rules Report"], rules_report_data, dict(wrap_text=True)
            )
            self._populate_metadata_sheets(
                wb,
                define_version,
                cdiscCt,
                standard,
                version,
                dictionary_versions,
                file_part=i + 1,
                total_parts=num_files,
                **kwargs,
            )
            workbooks.append(wb)
        return workbooks

    def _populate_metadata_sheets(
        self,
        wb: Workbook,
        define_version,
        cdiscCt,
        standard,
        version,
        dictionary_versions,
        file_part: int = None,
        total_parts: int = None,
        **kwargs,
    ):
        """
        Populate the conformance and dataset details sheets.
        """
        timestamp = datetime.now().replace(microsecond=0).isoformat()
        if file_part and total_parts:
            timestamp += f" (Part {file_part} of {total_parts})"
        wb["Conformance Details"]["B2"] = timestamp
        wb["Conformance Details"]["B3"] = f"{round(self._elapsed_time, 2)} seconds"
        wb["Conformance Details"]["B4"] = __version__

        # write dataset metadata
        datasets_data = [
            [
                dataset.filename,
                dataset.label,
                str(Path(dataset.full_path or "").parent),
                dataset.modification_date,
                (dataset.file_size or 0) / 1000,
                dataset.record_count,
            ]
            for dataset in self._datasets
        ]
        excel_update_worksheet(
            wb["Dataset Details"], datasets_data, dict(wrap_text=True)
        )

        # write standards details
        wb["Conformance Details"]["B7"] = standard.upper()
        if "substandard" in kwargs and kwargs["substandard"] is not None:
            wb["Conformance Details"]["B8"] = kwargs["substandard"]
        wb["Conformance Details"]["B9"] = f"V{version}"
        if cdiscCt:
            wb["Conformance Details"]["B10"] = (
                ", ".join(cdiscCt)
                if isinstance(cdiscCt, (list, tuple, set))
                else str(cdiscCt)
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

    def write_report(self, **kwargs):
        logger = logging.getLogger("validator")
        define_xml_path = kwargs.get("define_xml_path")
        dictionary_versions = kwargs.get("dictionary_versions", {})
        try:
            if define_xml_path:
                define_version = get_define_version([define_xml_path])
            else:
                define_version: str = self._args.define_version
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
            template_buffer = self._template.read()
            workbooks = self.get_export(
                define_version,
                controlled_terminology,
                self._args.standard,
                self._args.version.replace("-", "."),
                dictionary_versions,
                template_buffer,
                substandard=(
                    self._args.substandard
                    if hasattr(self._args, "substandard")
                    else None
                ),
            )
            if len(workbooks) == 1:
                # Single file - use original filename
                with open(self._output_name, "wb") as f:
                    f.write(excel_workbook_to_stream(workbooks[0]))
                logger.debug(f"Report written to: {self._output_name}")
            else:
                # Multiple files - add part numbers
                base_name = Path(self._output_name).stem
                extension = Path(self._output_name).suffix
                parent_dir = Path(self._output_name).parent

                for i, wb in enumerate(workbooks, 1):
                    filename = parent_dir / f"{base_name}_part{i}{extension}"
                    with open(filename, "wb") as f:
                        f.write(excel_workbook_to_stream(wb))
                    logger.debug(f"Report part {i} written to: {filename}")
                logger.warning(
                    f"Data exceeded Excel row limit. Created {len(workbooks)} report files. "
                    f"Total rows split across files."
                )
        except Exception as e:
            logger.error(f"Error writing report: {e}")
            raise e
        finally:
            self._template.close()
