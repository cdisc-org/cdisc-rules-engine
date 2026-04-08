import pandas as pd
from dataclasses import dataclass

from setuptools import glob
from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface


@dataclass
class MedRTVersionMetadata:
    version: str


class MedRTReader:
    def __init__(self, pgi: PostgresQLInterface, dictionary_path: str):
        self.pgi = pgi
        self.dictionary_path = dictionary_path

    def _extract_version_metadata(self) -> MedRTVersionMetadata:
        """Extract metadata from the MedRT Release txt file name."""
        file_path = glob.glob(f"{self.dictionary_path}/MEDRT_Release_Notes.txt")[0]
        with open(file_path, "r") as f:
            first_line = f.readline()
            version = first_line.split(" MED-RT ")[0]
            return MedRTVersionMetadata(version=version)

    def process_data(self, metadata: MedRTVersionMetadata = None) -> pd.DataFrame:
        """
        Reads the MedRT .txt file and returns a dataframe of the term code, term name, and term tag (if applicable)
        """
        # metadata object is not currently used but will be relevant when I plan to implement more version logic
        file_path = glob.glob(f"{self.dictionary_path}/MEDRT.txt")[0]
        df = pd.read_csv(file_path, sep="\t", header=None, usecols=[0, 1], dtype=str, encoding="utf-8")
        df.rename(columns={1: "term_code"}, inplace=True)
        df[["term_name", "term_tag"]] = df[0].str.extract(r"^(.*?)\s*\[(.*?)\]$")
        df = df[["term_code", "term_tag", "term_name"]]
        return df
