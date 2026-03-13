from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd

from cdisc_rules_engine.models.dataset_metadata2 import (
    DatasetMetadata2,
    VariableMetadata,
)


class BaseDataReader(ABC):
    """
    Abstract Reader for datasets
    """

    CHUNKSIZE = 10000

    SUPPORTED_EXTENSIONS = [".xpt", ".sas7bdat"]

    def __init__(self, file_path: str):
        if not file_path.lower().endswith(self._get_extension()):
            raise ValueError("Tried to read file with incorrect data reader")
        self.file_path = file_path

    @abstractmethod
    def read(self) -> Tuple[DatasetMetadata2, Iterable[List[Dict[str, Any]]]]:
        pass

    @abstractmethod
    def _get_extension(self) -> str:
        pass

    @abstractmethod
    def _extract_variable_metadata(self, reader) -> List[VariableMetadata]:
        pass

    def _extract_metadata(self, reader) -> Dict[str, Any]:
        """Read only the metadata."""
        path = Path(self.file_path)
        return DatasetMetadata2(
            filename=path.name.lower(),
            name=path.name.lower().replace(self._get_extension(), ""),
            variables=self._extract_variable_metadata(reader),
            # TODO: How to extract this?
            label="",
        )

    def _read_chunks(self, reader, metadata: DatasetMetadata2) -> Iterable[List[Dict[str, Any]]]:
        """Read the file as chunks into a stream."""
        for chunk_df in reader:
            chunk_df = self._cleanup_missing_values(chunk_df, metadata)
            chunk_df = self._cleanup_zero_floating_point_values(chunk_df, metadata)
            chunk_data = chunk_df.to_dict(orient="records")
            yield chunk_data

    def _cleanup_missing_values(self, df: pd.DataFrame, metadata: DatasetMetadata2) -> pd.DataFrame:
        """Clean up missing values based on column type, modifies the input dataframe."""
        for column in metadata.variables:
            df[column.name] = df[column.name].where(df[column.name].notna(), None)
            if column.type == "Char":
                df[column.name] = df[column.name].apply(lambda x: "" if pd.isna(x) or x is None else x)

        return df

    def _cleanup_zero_floating_point_values(self, df: pd.DataFrame, metadata: DatasetMetadata2) -> pd.DataFrame:
        """Clean up zero floating point values by replacing them with 0.0, modifies the input dataframe."""
        for column in metadata.variables:
            if column.type == "Num":
                df[column.name] = df[column.name].apply(
                    lambda x: 0.0 if isinstance(x, (int, float)) and abs(x) < 1e-20 else x
                )
        return df

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
