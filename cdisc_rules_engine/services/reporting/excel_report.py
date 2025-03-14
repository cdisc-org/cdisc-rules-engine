import logging
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

    @property
    def _file_format(self):
        return ReportTypes.XLSX.value.lower()

    def get_export(
        self, define_version, cdiscCt, standard, version, dictionary_versions, **kwargs
    ) -> Workbook:
        wb = excel_open_workbook(self._template.read())
        summary_data = self.get_summary_data()
        detailed_data = self.get_detailed_data(excel=True)
        rules_report_data = self.get_rules_report_data()
        excel_update_worksheet(wb["Issue Summary"], summary_data, dict(wrap_text=True))
        excel_update_worksheet(wb["Issue Details"], detailed_data, dict(wrap_text=True))
        excel_update_worksheet(
            wb["Rules Report"], rules_report_data, dict(wrap_text=True)
        )
        # write conformance data
        wb["Conformance Details"]["B2"] = (
            datetime.now().replace(microsecond=0).isoformat()
        )
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
        except Exception as e:
            logger.error(e)
            raise e
        finally:
            self._template.close()
