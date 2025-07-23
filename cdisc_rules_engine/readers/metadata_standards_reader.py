import re
from typing import List, Dict, Any, Tuple
from cdisc_rules_engine.readers.base_reader import BaseReader
from dataclasses import dataclass
import pandas as pd


@dataclass
class MetadataStandardMetadata:
    """Metadata extracted from IG filenames."""

    standard_type: str
    standard_name: str
    version: str
    extension: str


class MetadataStandardsReader(BaseReader):
    """
    Reader for CDISC Implementation Guide (IG) metadata standards files.
    Reads both Data Structures and Variables sheets from Excel files.
    """

    SDTM_COLUMN_MAPPING = {
        "Version": "version",
        "Variable Order": "variable_order",
        "Class": "class",
        "Dataset Name": "dataset_name",
        "Dataset Label": "dataset_label",
        "Variable Name": "variable_name",
        "Variable Label": "variable_label",
        "Structure": "structure",
        "Type": "type",
        "CDISC CT Codelist Code(s)": "codelist_code",
        "Codelist Submission Value(s)": "submission_value",
        "Described Value Domain(s)": "value_domain",
        "Value List": "value_list",
        "Role": "role",
        "CDISC Notes": "notes",
        "Core": "core",
    }

    ADAM_COLUMN_MAPPING = {
        "Version": "version",
        "Data Structure Name": "structure_name",
        "Data Structure Description": "structure_description",
        "Class": "class",
        "Subclass": "subclass",
        "Variable Set": "variable_set",
        "Variable Name": "variable_name",
        "Variable Label": "variable_label",
        "Type": "type",
        "CDISC CT Codelist Code(s)": "codelist_code",
        "Codelist Submission Value(s)": "submission_value",
        "Described Value Domain(s)": "value_domain",
        "Value List": "value_list",
        "CDISC Notes": "notes",
        "Core": "core",
    }

    FILENAME_PATTERN = re.compile(
        r"^(?P<standard_name>(?P<standard_type>ADaM|SDTM)IG(?:[-_][A-Z]+)?)_"
        r"(?P<version>v\d+\.\d+(?:\.\d+)?)"
        r"\.(?P<extension>xlsx|xls)$"
    )

    DATA_STRUCTURES_SHEET = "Data Structures"
    DATASETS_SHEET = "Datasets"
    VARIABLES_SHEET = "Variables"

    def _extract_metadata(self) -> MetadataStandardMetadata:
        """Extract metadata from the filename."""
        match = self.FILENAME_PATTERN.match(self.file_path.name)
        if not match:
            raise ValueError(
                f"Filename does not match expected pattern: {self.file_path.name}\n"
                f"Expected format: <STANDARD>IG(_/-)[SUFFIX]_v<VERSION>.<EXT>\n"
                f"Examples: ADaMIG_MD_v1.0.xlsx, SDTMIG-AP_v3.2.xlsx"
            )

        groups = match.groupdict()
        return MetadataStandardMetadata(
            standard_type=groups["standard_type"],
            standard_name=groups["standard_name"],
            version=groups["version"],
            extension=groups["extension"],
        )

    def read(self) -> Dict[str, List[Dict[str, Any]]]:
        """Read both sheets from the IG file and return formatted data."""
        datasets = self._read_data_structures()
        variables = self._read_variables()

        return {"datasets": datasets, "variables": variables}

    def _read_data_structures(self) -> List[Dict[str, Any]]:
        """Read and format the Data Structures sheet."""
        try:
            df = pd.read_excel(
                self.file_path,
                sheet_name=self.DATASETS_SHEET if self.metadata.standard_type == "SDTM" else self.DATA_STRUCTURES_SHEET,
            )
            df = df.rename(
                columns=self.SDTM_COLUMN_MAPPING if self.metadata.standard_type == "SDTM" else self.ADAM_COLUMN_MAPPING
            )
            raw_data = df.where(df.notna(), None).to_dict("records")
            return self._format_datasets_data(raw_data)
        except ValueError as e:
            if f"Worksheet named '{self.DATA_STRUCTURES_SHEET}' not found" in str(e):
                print(f"Warning: No '{self.DATA_STRUCTURES_SHEET}' sheet found in {self.file_path.name}")
                return []
            raise

    def _read_variables(self) -> List[Dict[str, Any]]:
        """Read and format the Variables sheet."""
        try:
            df = pd.read_excel(self.file_path, sheet_name=self.VARIABLES_SHEET)
            df = df.rename(
                columns=self.SDTM_COLUMN_MAPPING if self.metadata.standard_type == "SDTM" else self.ADAM_COLUMN_MAPPING
            )
            raw_data = df.where(df.notna(), None).to_dict("records")
            return self._format_variables_data(raw_data)
        except ValueError as e:
            if f"Worksheet named '{self.VARIABLES_SHEET}' not found" in str(e):
                print(f"Warning: No '{self.VARIABLES_SHEET}' sheet found in {self.file_path.name}")
                return []
            raise

    def _format_datasets_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format data from Data Structures and Datasets sheets for SQL insertion."""
        formatted_data = []

        for row in raw_data:
            if self.metadata.standard_type == "SDTM":
                formatted_row = {
                    "standard_type": self.metadata.standard_type,
                    "version": self.metadata.version,
                    "class": row.get("class"),
                    "dataset_name": row.get("dataset_name"),
                    "dataset_label": row.get("dataset_label"),
                    "structure": row.get("structure"),
                }
                formatted_data.append(formatted_row)
            else:  # ADaM
                formatted_row = {
                    "standard_type": self.metadata.standard_type,
                    "version": self.metadata.version,
                    "structure_name": row.get("structure_name"),
                    "structure_description": row.get("structure_description"),
                    "class": row.get("class"),
                    "subclass": row.get("subclass"),
                    "notes": row.get("notes"),
                }
                formatted_data.append(formatted_row)

        return formatted_data

    def _format_variables_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format data from Variables sheet for SQL insertion."""
        formatted_data = []

        for row in raw_data:
            if self.metadata.standard_type == "SDTM":
                formatted_row = {
                    "standard_type": self.metadata.standard_type,
                    "version": self.metadata.version,
                    "variable_order": row.get("variable_order"),
                    "class": row.get("class"),
                    "dataset_name": row.get("dataset_name"),
                    "variable_name": row.get("variable_name"),
                    "variable_label": row.get("variable_label"),
                    "type": row.get("type"),
                    "codelist_code": row.get("codelist_code"),
                    "submission_value": row.get("submission_value"),
                    "value_domain": row.get("value_domain"),
                    "value_list": row.get("value_list"),
                    "role": row.get("role"),
                    "notes": row.get("notes"),
                    "core": row.get("core"),
                }
                formatted_data.append(formatted_row)
            else:  # ADaM
                formatted_row = {
                    "standard_type": self.metadata.standard_type,
                    "version": self.metadata.version,
                    "structure_name": row.get("structure_name"),
                    "variable_set": row.get("variable_set"),
                    "variable_name": row.get("variable_name"),
                    "variable_label": row.get("variable_label"),
                    "type": row.get("type"),
                    "codelist_code": row.get("codelist_code"),
                    "submission_value": row.get("submission_value"),
                    "value_domain": row.get("value_domain"),
                    "value_list": row.get("value_list"),
                    "core": row.get("core"),
                    "notes": row.get("notes"),
                }
                formatted_data.append(formatted_row)

        return formatted_data

    def get_table_names(self) -> Tuple[str, str]:
        """Return the appropriate table names for this data."""
        return ("ig_datasets", "ig_variables")
