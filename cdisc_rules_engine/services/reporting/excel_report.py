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

    DEFAULT_MAX_ROWS = 10000

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
        env_max_rows = (
            int(os.getenv("MAX_REPORT_ROWS")) if os.getenv("MAX_REPORT_ROWS") else None
        )
        if env_max_rows is not None and args.max_report_rows is not None:
            result = max(env_max_rows, args.max_report_rows)
        elif env_max_rows is not None:
            result = env_max_rows
        elif args.max_report_rows is not None:
            result = args.max_report_rows
        else:
            result = self.DEFAULT_MAX_ROWS
        if result == 0:
            result = None
        elif result < 0:
            result = self.DEFAULT_MAX_ROWS
        self.max_rows_per_sheet = result

    @property
    def _file_format(self):
        return ReportTypes.XLSX.value.lower()

    def get_export(
        self,
        define_version,
        cdiscCt,
        standard,
        version,
        dictionary_versions,
        **kwargs,
    ) -> Workbook:
        logger = logging.getLogger("validator")
        summary_data = self.get_summary_data()
        detailed_data = self.get_detailed_data(excel=True)
        rules_report_data = self.get_rules_report_data()

        total_rows = len(detailed_data)
        if self.max_rows_per_sheet is not None and total_rows > self.max_rows_per_sheet:
            detailed_data = detailed_data[: self.max_rows_per_sheet]
            logger.warning(
                f"Issue Details truncated to limit of {self.max_rows_per_sheet} rows. "
                f"Total issues found: {total_rows}"
            )
        wb = excel_open_workbook(self._template.read())
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

    def _populate_metadata_sheets(
        self,
        wb: Workbook,
        define_version,
        cdiscCt,
        standard,
        version,
        dictionary_versions,
        **kwargs,
    ):
        """
        Populate the conformance and dataset details sheets.
        """
        timestamp = datetime.now().replace(microsecond=0).isoformat()
        wb["Conformance Details"]["B2"] = timestamp
        wb["Conformance Details"]["B3"] = f"{round(self._elapsed_time, 2)} seconds"
        wb["Conformance Details"]["B4"] = __version__
        wb["Conformance Details"]["B5"] = str(self._max_errors_limit)
        wb["Conformance Details"]["B6"] = (
            "None"
            if self._errors_per_dataset_flag == 0
            else str(self._errors_per_dataset_flag)
        )
        wb["Conformance Details"]["B7"] = str(self.max_rows_per_sheet)

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
        wb["Conformance Details"]["B9"] = standard.upper()
        if "substandard" in kwargs and kwargs["substandard"] is not None:
            wb["Conformance Details"]["B10"] = kwargs["substandard"]
        wb["Conformance Details"]["B11"] = f"V{version}"
        if cdiscCt:
            wb["Conformance Details"]["B12"] = (
                ", ".join(cdiscCt)
                if isinstance(cdiscCt, (list, tuple, set))
                else str(cdiscCt)
            )
        else:
            wb["Conformance Details"]["B12"] = ""
        wb["Conformance Details"]["B13"] = define_version

        # Populate external dictionary versions
        unii_version = dictionary_versions.get(DictionaryTypes.UNII.value)
        if unii_version is not None:
            wb["Conformance Details"]["B14"] = unii_version

        medrt_version = dictionary_versions.get(DictionaryTypes.MEDRT.value)
        if medrt_version is not None:
            wb["Conformance Details"]["B15"] = medrt_version

        meddra_version = dictionary_versions.get(DictionaryTypes.MEDDRA.value)
        if meddra_version is not None:
            wb["Conformance Details"]["B16"] = meddra_version

        whodrug_version = dictionary_versions.get(DictionaryTypes.WHODRUG.value)
        if whodrug_version is not None:
            wb["Conformance Details"]["B17"] = whodrug_version

        snomed_version = dictionary_versions.get(DictionaryTypes.SNOMED.value)
        if snomed_version is not None:
            wb["Conformance Details"]["B18"] = snomed_version
        return wb

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
            report_data = self.get_export(
                define_version,
                controlled_terminology,
                self._args.standard,
                self._args.version.replace("-", "."),
                dictionary_versions,
                substandard=(
                    self._args.substandard
                    if hasattr(self._args, "substandard")
                    else None
                ),
            )
            with open(self._output_name, "wb") as f:
                f.write(excel_workbook_to_stream(report_data))
            logger.debug(f"Report written to: {self._output_name}")
        except Exception as e:
            logger.error(f"Error writing report: {e}")
            raise e
        finally:
            self._template.close()
