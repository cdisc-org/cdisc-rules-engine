from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict, Any
import csv


class BaseReader(ABC):
    """
    Abstract base class for all data readers.
    Provides common functionality for reading and parsing files.
    """

    def __init__(self, file_path: str):
        """Initialise the reader with a file path."""
        self.file_path = Path(file_path)
        self._validate_file()
        self.metadata = self._extract_metadata()

    def _validate_file(self) -> None:
        """Validate that the file exists and is accessible."""
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")
        if not self.file_path.is_file():
            raise ValueError(f"Path is not a file: {self.file_path}")

    @abstractmethod
    def _extract_metadata(self) -> Dict[str, Any]:
        """
        Extract metadata from the file name or content.
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def read(self) -> List[Dict[str, Any]]:
        """
        Read the file and return serialised data.
        Must be implemented by subclasses.
        """
        pass

    def _read_csv(self) -> List[Dict[str, Any]]:
        """
        Common CSV reading functionality.
        Used by subclasses to read CSV files (e.g. metadata standards, terminology).
        """
        data = []
        with open(self.file_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                cleaned_row = {k: v.strip() if v else None for k, v in row.items()}
                data.append(cleaned_row)
        return data
