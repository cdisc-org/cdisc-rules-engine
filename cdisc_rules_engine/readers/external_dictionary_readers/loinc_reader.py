import os
import pandas as pd
from dataclasses import dataclass
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface


@dataclass
class LoincVersionMetadata:
    version: str


class LoincReader:
    def __init__(self, pgi: PostgresQLInterface, dictionary_path: str):
        self.pgi = pgi
        self.dictionary_path = dictionary_path

    def _extract_version_metadata(self) -> LoincVersionMetadata:
        """Extract metadata from the LOINC directory."""
        base_dir = os.path.basename(os.path.normpath(self.dictionary_path))

        if "_" in base_dir:
            version = base_dir.split("_")[-1]
        else:
            for file in os.listdir(self.dictionary_path):
                if file.startswith("Loinc_") and file.endswith("_DifferenceReport.pdf"):
                    version = file.split("_")[1]
                    break
            else:
                version = "unknown"
        return LoincVersionMetadata(version=version)

    def process_data(self, metadata: LoincVersionMetadata = None) -> pd.DataFrame:
        """
        Reads the Loinc.csv file and returns a dataframe with the mapped term code, term name, version.
        """
        file_path = f"{self.dictionary_path}/LoincTable/Loinc.csv"
        if not os.path.exists(file_path):
            file_path = f"{self.dictionary_path}/Loinc.csv"

        df = pd.read_csv(file_path, dtype=str, encoding="utf-8")

        df.rename(
            columns={
                "LOINC_NUM": "term_code",
                "COMPONENT": "term_name",
                "VersionLastChanged": "version",
            },
            inplace=True,
        )

        return df
