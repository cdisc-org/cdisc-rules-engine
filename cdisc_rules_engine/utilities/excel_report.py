from datetime import datetime
from openpyxl import Workbook
from cdisc_rules_engine.utilities.excel_writer import (
    excel_open_workbook,
    excel_update_worksheet,
)
from cdisc_rules_engine.utilities.generic_report import GenericReport


class ExcelReport(GenericReport):
    """
    Generates an excel report for a given set of validation results.
    """

    def get_excel_export(self, define_version, cdiscCt, standard, version) -> Workbook:
        """
        Populates the excel workbook template found in the file "CORE-Report-Template.xlsx" with
        data from validation results
        """
        wb = excel_open_workbook(self._template)
        summary_data = self.get_summary_data()
        detailed_data = self.get_detailed_data()
        rules_report_data = self.get_rules_report_data()
        excel_update_worksheet(wb["Issue Summary"], summary_data, dict(wrap_text=True))
        excel_update_worksheet(wb["Issue Details"], detailed_data, dict(wrap_text=True))
        excel_update_worksheet(
            wb["Rules Report"], rules_report_data, dict(wrap_text=True)
        )
        wb["Conformance Details"]["B2"] = self._data_path
        # write conformance data
        wb["Conformance Details"]["B3"] = (
            datetime.now().replace(microsecond=0).isoformat()
        )
        wb["Conformance Details"]["B4"] = f"{round(self._elapsed_time, 2)} seconds"
        # write bundle details
        wb["Conformance Details"]["B8"] = standard.upper()
        wb["Conformance Details"]["B9"] = f"V{version}"
        wb["Conformance Details"]["B10"] = ", ".join(cdiscCt)
        wb["Conformance Details"]["B11"] = define_version
        return wb
