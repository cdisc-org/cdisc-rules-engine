from dataclasses import dataclass
from typing import List, Dict, Any, Iterator, Optional

import pandas as pd
from pandas.io.sas.sas7bdat import SAS7BDATReader
from pandas.io.sas.sas_xport import XportReader

from cdisc_rules_engine.readers.base_reader import BaseReader


@dataclass
class ClinicalDataMetadata:
    """Metadata extracted from clinical data filenames and content."""

    domain: str
    standard_type: str
    file_format: str


class DataReader(BaseReader):
    """
    Reader for clinical data files (XPT and SAS7BDAT).
    Handles both ADaM and SDTM datasets.
    """

    CHUNKSIZE = 10000

    ADAM_DOMAINS = ["adae", "adef", "adsl", "adtte"]
    SDTM_DOMAINS = ["ae", "dm", "ex", "lb", "suppdm", "ta", "td", "te", "ti", "ts", "tv", "xp"]

    SUPPORTED_EXTENSIONS = [".xpt", ".sas7bdat"]

    def __init__(self, file_path: str):
        super().__init__(file_path)
        self._column_types: Optional[Dict[str, str]] = None

    def read_metadata(self) -> Dict[str, Any]:
        """Read only the metadata."""
        if self.file_path.suffix == ".xpt":
            reader = XportReader(self.file_path, encoding="utf-8")
        elif self.file_path.suffix == ".sas7bdat":
            reader = SAS7BDATReader(self.file_path, encoding="utf-8")
        else:
            raise ValueError(f"Unsupported file format: {self.file_path.suffix}")

        try:
            variable_metadata = self._extract_variable_metadata(reader)
            record_count = self._get_record_count(reader)

            self._column_types = self._extract_column_types(variable_metadata)

            study_id = None
            try:
                first_record = self._get_first_record()
                if first_record and "STUDYID" in first_record:
                    study_id = str(first_record["STUDYID"])
            except Exception:
                pass

            return {
                "metadata": {
                    "name": self.file_path.name,
                    "domain": self.metadata.domain,
                    "standard_type": self.metadata.standard_type,
                    "file_format": self.metadata.file_format,
                    "study_id": study_id,
                    "record_count": record_count,
                    "variable_count": len(variable_metadata),
                },
                "variables": variable_metadata,
            }
        finally:
            reader.close()

    def read(self) -> Iterator[List[Dict[str, Any]]]:
        """Read and stream records in chunks."""
        if self._column_types is None:
            metadata = self.read_metadata()
            self._column_types = self._extract_column_types(metadata["variables"])

        yield from self._read_chunks(chunksize=self.CHUNKSIZE)

    def _read_chunks(self, chunksize: int = CHUNKSIZE) -> Iterator[List[Dict[str, Any]]]:
        """Read the file in chunks."""
        reader = pd.read_sas(
            self.file_path,
            format="xport" if self.file_path.suffix == ".xpt" else "sas7bdat",
            encoding="utf-8",
            chunksize=chunksize,
        )

        for chunk_df in reader:
            chunk_df = self._cleanup_missing_values(chunk_df)
            chunk_data = chunk_df.to_dict(orient="records")
            yield chunk_data

    def _cleanup_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean up missing values based on column type."""
        if self._column_types is None:
            return df.where(df.notna(), None)

        cleaned_df = df.copy()

        for column in cleaned_df.columns:
            column_type = self._column_types.get(column, "unknown")

            if self._is_numeric_type(column_type):
                cleaned_df[column] = cleaned_df[column].where(cleaned_df[column].notna(), None)
            elif self._is_text_type(column_type):
                cleaned_df[column] = cleaned_df[column].fillna("")
                cleaned_df[column] = cleaned_df[column].apply(lambda x: "" if pd.isna(x) or x is None else x)
            else:
                cleaned_df[column] = cleaned_df[column].where(cleaned_df[column].notna(), None)

        return cleaned_df

    def _extract_column_types(self, variable_metadata: List[Dict[str, Any]]) -> Dict[str, str]:
        """Extract column types from variable metadata."""
        column_types = {}
        for var in variable_metadata:
            name = var.get("name", "")
            sas_type = var.get("type", "")
            column_types[name] = sas_type
            column_types[name.upper()] = sas_type
        return column_types

    def _is_text_type(self, sas_type: str) -> bool:
        """Determine if a SAS type represents a text/character column."""
        if not sas_type:
            return False

        type_lower = str(sas_type).lower().strip()
        return type_lower in ["s", "string", "c", "char", "character", "text"]

    def _is_numeric_type(self, sas_type: str) -> bool:
        """Determine if a SAS type represents a numeric column."""
        if not sas_type:
            return False

        type_lower = str(sas_type).lower().strip()
        return type_lower in ["d", "double", "n", "num", "numeric", "i", "int", "integer", "f", "float"]

    def _get_first_record(self) -> Dict[str, Any]:
        """Get the first record from the file."""
        df = pd.read_sas(
            self.file_path, format="xport" if self.file_path.suffix == ".xpt" else "sas7bdat", encoding="utf-8", nrows=1
        )
        if not df.empty:
            if self._column_types is None:
                reader = (
                    XportReader(self.file_path, encoding="utf-8")
                    if self.file_path.suffix == ".xpt"
                    else SAS7BDATReader(self.file_path, encoding="utf-8")
                )
                try:
                    variable_metadata = self._extract_variable_metadata(reader)
                    self._column_types = self._extract_column_types(variable_metadata)
                finally:
                    reader.close()

            df = self._cleanup_missing_values(df)
            return df.iloc[0].to_dict()
        return {}

    def _extract_metadata(self) -> ClinicalDataMetadata:
        """Extract metadata from the file name or content."""
        file_name = self.file_path.stem.lower()
        domain = None
        standard_type = None
        file_format = self.file_path.suffix[1:]

        if any(d in file_name for d in self.ADAM_DOMAINS):
            standard_type = "ADaM"
            domain = next((d for d in self.ADAM_DOMAINS if d in file_name), None)
        elif any(d in file_name for d in self.SDTM_DOMAINS):
            standard_type = "SDTM"
            domain = next((d for d in self.SDTM_DOMAINS if d in file_name), None)

        if not domain or not standard_type:
            raise ValueError(f"Unsupported domain or standard type in file name: {file_name}")

        return ClinicalDataMetadata(domain=domain, standard_type=standard_type, file_format=file_format)

    def _extract_variable_metadata(self, reader) -> List[Dict[str, Any]]:
        """Extract variable-level metadata from SAS reader."""
        variables = []

        if isinstance(reader, SAS7BDATReader):
            for i, col in enumerate(reader.columns):
                var_info = {
                    "order": i + 1,
                    "col_id": col.col_id,
                    "name": col.name.decode("utf-8").strip() if isinstance(col.name, bytes) else col.name,
                    "label": (
                        col.label.decode("utf-8").strip() if col.label and isinstance(col.label, bytes) else col.label
                    ),
                    "format": (
                        col.format.decode("utf-8").strip()
                        if col.format and isinstance(col.format, bytes)
                        else col.format
                    ),
                    "type": (
                        col.ctype.decode("utf-8").strip() if col.ctype and isinstance(col.ctype, bytes) else col.ctype
                    ),
                    "length": col.length,
                }
                variables.append(var_info)
        elif isinstance(reader, XportReader):
            for i, field in enumerate(reader.fields):
                var_info = {
                    "order": i + 1,
                    "name": field.get("name", "").strip(),
                    "label": field.get("label", ""),
                    "format": field.get("nform", ""),
                    "type": field.get("ntype", ""),
                    "length": field.get("field_length", 0),
                }
                variables.append(var_info)
        else:
            raise ValueError(f"Unsupported SAS reader type: {type(reader)}")

        return variables

    def _get_record_count(self, reader) -> int:
        """Get record count without loading all data."""
        if isinstance(reader, SAS7BDATReader):
            return reader.row_count
        else:
            count = 0
            chunk_reader = pd.read_sas(self.file_path, format="xport", encoding="utf-8", chunksize=self.CHUNKSIZE)
            for chunk in chunk_reader:
                count += len(chunk)
            return count
