import re
from datetime import datetime
from typing import List, Dict, Any
from cdisc_rules_engine.readers.base_reader import BaseReader
from dataclasses import dataclass
import pandas as pd


@dataclass
class CodelistMetadata:
    """Metadata extracted from codelist filenames."""

    standard_type: str
    version_date: str
    extension: str


class CodelistReader(BaseReader):
    """
    Reader for CDISC Controlled Terminology codelist files.
    Handles both codelist and code_list_item files in CSV and Excel formats.
    """

    FILENAME_PATTERN = re.compile(
        r"^(?P<standard_type>ADaM|SDTM)_CT_"
        r"(?P<version_date>\d{8}|\d{4}-\d{2}-\d{2})"
        r"\.(?P<extension>csv|tsv|xlsx|xls)$"
    )

    EXCEL_COLUMN_MAPPING = {
        "Code": "item_code",
        "Codelist Code": "codelist_code",
        "Codelist Extensible (Yes/No)": "extensible",
        "Codelist Name": "name",
        "CDISC Submission Value": "value",
        "CDISC Synonym(s)": "synonym",
        "CDISC Definition": "definition",
        "NCI Preferred Term": "term",
        "Standard and Date": "standard_and_date",
    }

    def _extract_metadata(self) -> CodelistMetadata:
        """Extract metadata from the filename."""
        match = self.FILENAME_PATTERN.match(self.file_path.name)
        if not match:
            raise ValueError(
                f"Filename does not match expected pattern: {self.file_path.name}\n"
                "Expected formats:\n"
                "  - <STANDARD>_CT_<YYYYMMDD>.<EXT>\n"
                "  - <STANDARD>_CT_<YYYY-MM-DD>.<EXT>"
            )

        groups = match.groupdict()

        return CodelistMetadata(
            standard_type=groups["standard_type"],
            version_date=groups["version_date"],
            extension=groups["extension"],
        )

    def _format_version_date(self, date_str: str) -> str:
        """Convert YYYYMMDD to YYYY-MM-DD format."""
        if "-" in date_str:
            return date_str

        try:
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date format: {date_str}")

    def _read_excel_with_sheet(self) -> List[Dict[str, Any]]:
        """Read Excel file from the terminology sheet and normalise column names."""
        try:
            df = pd.read_excel(self.file_path, sheet_name="Terminology")
            df = df.rename(columns=self.EXCEL_COLUMN_MAPPING)

            return df.where(df.notna(), None).to_dict("records")
        except ValueError as e:
            if "Worksheet named 'Terminology' not found" in str(e):
                df = pd.read_excel(self.file_path)
                df = df.rename(columns=self.EXCEL_COLUMN_MAPPING)
                return df.where(df.notna(), None).to_dict("records")
            raise

    def read(self) -> List[Dict[str, Any]]:
        """Read the file and return data formatted for SQL insertion."""
        if self.metadata.extension in ["xlsx", "xls"]:
            raw_data = self._read_excel_with_sheet()
        elif self.metadata.extension == "csv":
            raw_data = self._read_excel()
        else:
            raise ValueError(f"Unsupported file extension: {self.metadata.extension}")
        return self._format_data(raw_data)

    def _format_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format data for the codelist table."""
        formatted_data = []
        version_date = self._format_version_date(self.metadata.version_date)

        for row in raw_data:
            formatted_row = {
                "standard_type": self.metadata.standard_type,
                "version_date": version_date,
                "item_code": row.get("item_code"),
                "codelist_code": row.get("codelist_code"),
                "extensible": row.get("extensible"),
                "name": row.get("name"),
                "value": row.get("value"),
                "synonym": row.get("synonym"),
                "definition": row.get("definition"),
                "term": row.get("term"),
                "standard_and_date": row.get("standard_and_date"),
            }
            formatted_data.append(formatted_row)

        return formatted_data

    def _format_code_list_item_data(
        self, raw_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Format data for the code_list_item table."""
        formatted_data = []
        version_date = self._format_version_date(self.metadata.version_date)

        for row in raw_data:
            formatted_row = {
                "standard_type": self.metadata.standard_type,
                "version_date": version_date,
                "item_code": row.get("item_code"),
                "codelist_code": row.get("codelist_code"),
                "name": row.get("name"),
                "value": row.get("value"),
                "synonym": row.get("synonym"),
                "definition": row.get("definition"),
                "term": row.get("term"),
                "standard_and_date": row.get("standard_and_date"),
            }
            formatted_data.append(formatted_row)

        return formatted_data
