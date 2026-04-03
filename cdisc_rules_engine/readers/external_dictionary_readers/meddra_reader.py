import os
import pandas as pd
from dataclasses import dataclass
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface


@dataclass
class MeddraVersionMetadata:
    version: str
    language: str


class MeddraReader:
    def __init__(self, pgi: PostgresQLInterface, dictionary_path: str):
        self.pgi = pgi
        self.dictionary_path = dictionary_path

    def _extract_version_metadata(self) -> MeddraVersionMetadata:
        """Extract metadata from the MedDRA release file."""
        file_path = f"{self.dictionary_path}/meddra_release.asc"
        with open(file_path, mode="r", encoding="utf-8") as f:
            content = f.read().strip().split("$")
            return MeddraVersionMetadata(version=content[0], language=content[1])

    def process_data(self, metadata: MeddraVersionMetadata = None) -> pd.DataFrame:
        """
        Reads the 5 term .asc files and concatenates them into a unified dataframe.
        """
        # metadata object is not currently used but will be relevant when I plan to implement more version logic
        term_files = {"SOC": "soc.asc", "HLGT": "hlgt.asc", "HLT": "hlt.asc", "PT": "pt.asc", "LLT": "llt.asc"}

        dfs = []
        for term_type, file_name in term_files.items():
            path = f"{self.dictionary_path}/{file_name}"
            if os.path.exists(path):
                df = pd.read_csv(path, sep="$", header=None, usecols=[0, 1], dtype=str, encoding="utf-8")
                df.columns = ["term_code", "term_name"]
                df["term_type"] = term_type
                dfs.append(df)

        return pd.concat(dfs, ignore_index=True)
