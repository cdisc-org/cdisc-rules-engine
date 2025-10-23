import logging
from typing import BinaryIO, List, Optional, override
import os

from openpyxl import Workbook
from cdisc_rules_engine.enums.report_types import ReportTypes
from cdisc_rules_engine.models.validation_args import Validation_args
from cdisc_rules_engine.services.reporting.base_report_data import (
    BaseReportData,
)
from .base_report import BaseReport
from .excel_writer import (
    excel_open_workbook,
    excel_update_worksheet,
    excel_workbook_to_stream,
)
from pathlib import Path


class ExcelReport(BaseReport):
    """
    Generates an excel report for a given set of validation results.
    """

    DEFAULT_MAX_ROWS = 1000000

    def __init__(
        self,
        report_standard: BaseReportData,
        args: Validation_args,
        template: Optional[BinaryIO] = None,
    ):
        super().__init__(
            report_standard,
            args,
            template,
        )
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
    @override
    def _file_ext(self):
        return ReportTypes.XLSX.value.lower()

    def _chunk_data(self, data: List[List], chunk_size: int) -> List[List[List]]:
        chunks = []
        for i in range(0, len(data), chunk_size):
            chunks.append(data[i : i + chunk_size])
        return chunks

    def _needs_splitting(self) -> bool:
        if self.max_rows_per_sheet is None:
            return False
        for data_sheet in self._report_standard.data_sheets.values():
            if len(data_sheet) > self.max_rows_per_sheet:
                return True
        return False

    @override
    def get_export(
        self,
        template_buffer,
    ) -> List[Workbook]:

        if not self._needs_splitting():
            # Single file - original behavior
            wb = self._create_single_workbook(template_buffer)
            return [wb]
        else:
            # Multiple files needed
            return self._create_multiple_workbooks(template_buffer)

    def _create_single_workbook(
        self,
        template_buffer,
    ) -> Workbook:
        wb = excel_open_workbook(template_buffer)
        for sheet_name, data_sheet in self._report_standard.data_sheets.items():
            excel_update_worksheet(wb[sheet_name], data_sheet, {"wrap_text": True})
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
        summary_needs_split = len(summary_chunks) > 1
        num_files = max(len(detailed_chunks), len(summary_chunks))
        for i in range(num_files):
            wb = excel_open_workbook(template_buffer)
            if summary_needs_split:
                summary_chunk = summary_chunks[i] if i < len(summary_chunks) else []
            else:
                summary_chunk = summary_data
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

    @override
    def write_report(self):
        logger = logging.getLogger("validator")
        try:
            template_buffer = self._template.read()
            workbooks = self.get_export(
                template_buffer,
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
