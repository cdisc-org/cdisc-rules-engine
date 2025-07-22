import re
from datetime import datetime
from typing import List, Dict, Any
from base_reader import BaseReader
from dataclasses import dataclass


@dataclass
class CodelistMetadata:
    """Metadata extracted from codelist filenames."""

    standard_type: str
    version_date: str
    file_type: str


class CodelistReader(BaseReader):
    """
    Reader for CDISC Controlled Terminology codelist CSV files.
    Handles both codelist and code_list_item files.
    """

    # Regex pattern to extract metadata from filename
    # Example: ADaM_CT_20250328_terminology_codelist.csv
    FILENAME_PATTERN = re.compile(
        r"^(?P<standard_type>ADaM|SDTM)_CT_"
        r"(?P<version_date>\d{8})_terminology_"
        r"(?P<file_type>codelist|code_list_item)\.csv$"
    )

    def _extract_metadata(self) -> CodelistMetadata:
        """Extract metadata from the filename."""
        match = self.FILENAME_PATTERN.match(self.file_path.name)
        if not match:
            raise ValueError(
                f"Filename does not match expected pattern: {self.file_path.name}\n"
                f"Expected format: <STANDARD>_CT_<YYYYMMDD>_terminology_<TYPE>.csv"
            )

        groups = match.groupdict()
        return CodelistMetadata(
            standard_type=groups["standard_type"],
            version_date=groups["version_date"],
            file_type=groups["file_type"],
        )

    def _format_version_date(self, date_str: str) -> str:
        """Convert YYYYMMDD to YYYY-MM-DD format."""
        try:
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date format: {date_str}")

    def read(self) -> List[Dict[str, Any]]:
        """Read the CSV file and return data formatted for SQL insertion."""
        raw_data = self._read_csv()
        if self.metadata.file_type == "codelist":
            return self._format_codelist_data(raw_data)
        else:
            return self._format_code_list_item_data(raw_data)

    def _format_codelist_data(
        self, raw_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Format data for the codelist table."""
        formatted_data = []
        version_date = self._format_version_date(self.metadata.version_date)

        for row in raw_data:
            formatted_row = {
                "standard_type": self.metadata.standard_type,
                "version_date": version_date,
                "code": row.get("code"),
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

    def get_table_name(self) -> str:
        """Return the appropriate table name for this file type."""
        return self.metadata.file_type.replace("_", "_")
