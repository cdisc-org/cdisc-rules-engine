import os
import pandas as pd
from dataclasses import dataclass

from setuptools import glob
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface


@dataclass
class UniiVersionMetadata:
    version: str


class UniiReader:
    def __init__(self, pgi: PostgresQLInterface, dictionary_path: str):
        self.pgi = pgi
        self.dictionary_path = dictionary_path

    def _extract_version_metadata(self) -> UniiVersionMetadata:
        """Extract metadata from the UNII records txt file name."""
        file_path = glob.glob(f"{self.dictionary_path}/UNII_Records_*.txt")[0]
        version = os.path.basename(file_path).split("_")[2].split(".")[0]
        return UniiVersionMetadata(version=version)

    def process_data(self, metadata: UniiVersionMetadata = None) -> pd.DataFrame:
        """
        Reads the UNII records .txt file and returns a dataframe of the term code and term name
        """
        # metadata object is not currently used but will be relevant when I plan to implement more version logic
        file_path = glob.glob(f"{self.dictionary_path}/UNII_Records_*.txt")[0]
        df = pd.read_csv(file_path, sep="\t", header=None, usecols=[0, 1], dtype=str, encoding="utf-8")
        df = df.iloc[1:]
        df.columns = ["term_code", "term_name"]
        return df
