from dataclasses import dataclass
from typing import List, Dict, Any, Iterator

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
            chunk_data = chunk_df.where(chunk_df.notna(), None).to_dict(orient="records")
            yield chunk_data

    def _get_first_record(self) -> Dict[str, Any]:
        """Get the first record from the file."""
        df = pd.read_sas(
            self.file_path, format="xport" if self.file_path.suffix == ".xpt" else "sas7bdat", encoding="utf-8", nrows=1
        )
        if not df.empty:
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
