import re
import pandas as pd
from dataclasses import dataclass
from typing import Optional, List
from pathlib import Path
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface


@dataclass
class SnomedVersionMetadata:
    edition: int
    language: str
    package: str
    version: int


class SnomedReader:
    FILE_NAME_PATTERN = (
        r"sct(?P<edition>[0-9])_[a-zA-Z]+_Full-(?P<language>[a-z]{2})_(?P<package>[A-Z]+)_(?P<version>[0-9]{8})\.txt"
    )

    def __init__(self, pgi: PostgresQLInterface, dictionary_path: str):
        self.pgi = pgi
        self.dictionary_path = Path(dictionary_path)

    def _get_all_files(self) -> List[Path]:
        return list(self.dictionary_path.rglob("*.txt"))

    def _extract_version_metadata(self) -> SnomedVersionMetadata:
        for file_path in self._get_all_files():
            match = re.match(self.FILE_NAME_PATTERN, file_path.name)
            if match:
                return SnomedVersionMetadata(
                    edition=int(match.group("edition")),
                    language=match.group("language"),
                    package=match.group("package"),
                    version=int(match.group("version")),
                )
        raise FileNotFoundError(f"Could not find valid SNOMED RF files in {self.dictionary_path}")

    def _load_snomed_file(self, pattern: str, tag: str) -> pd.DataFrame:
        for file_path in self._get_all_files():
            if re.match(pattern, file_path.name):
                df = pd.read_csv(file_path, sep="\t", usecols=["id", "term"], dtype=str, encoding="utf-8")
                df = df.rename(columns={"id": "term_code", "term": "term_name"})
                df = df.drop_duplicates()
                df["term_tag"] = tag
                return df
        return pd.DataFrame(columns=["term_code", "term_name", "term_tag"])

    def process_data(self, metadata: Optional[SnomedVersionMetadata] = None) -> pd.DataFrame:
        if not metadata:
            metadata = self._extract_version_metadata()

        base_pattern = (
            f"sct{metadata.edition}_(Description|TextDefinition)_"
            f"Full-{metadata.language}_{metadata.package}_{metadata.version}.txt"
        )

        desc_df = self._load_snomed_file(base_pattern.format("Description"), "Description")
        text_df = self._load_snomed_file(base_pattern.format("TextDefinition"), "TextDefinition")

        return pd.concat([desc_df, text_df], ignore_index=True)
