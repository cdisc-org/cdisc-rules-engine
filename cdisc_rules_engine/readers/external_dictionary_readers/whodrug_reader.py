import os
import csv
import re
import pandas as pd
from datetime import datetime
from dataclasses import dataclass

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.enums.whodrug_files import (
    WhoDrugFormats,
    UniversalWhoDrugFiles,
    B3WHODrugFiles,
    C3WHODrugFiles,
)


@dataclass
class WhoDrugVersionMetadata:
    """Metadata extracted from codelist filenames."""

    product_type: str
    format: str
    version_date: str


class WhoDrugReader:
    """Reader for the WHO Drug dictionary."""

    def __init__(self, pgi: PostgresQLInterface, dictionary_path: str):
        self.pgi = pgi
        self.dictionary_path = dictionary_path

    def _extract_version_metadata(self) -> WhoDrugVersionMetadata:
        """Extract metadata from the Version file."""
        file_path = f"{self.dictionary_path}/{UniversalWhoDrugFiles.VERSION.value}.csv"
        with open(file_path, mode="r", encoding="utf-8") as f:
            reader = csv.reader(f)
            version_info = next(reader)[0]

        version_pattern = (
            r"WHODRUG\s+(?P<product_type>\w+)\s+(?P<format>C3|B3)\s+(?P<version_date>\w+\s\d{1,2},\s\d{4})"
        )
        match = re.match(version_pattern, version_info)
        if match:
            product_type = match.group("product_type")
            format = match.group("format")
            version_date = datetime.strptime(match.group("version_date"), "%B %d, %Y").strftime("%Y%m%d")
            return WhoDrugVersionMetadata(product_type=product_type, format=format, version_date=version_date)
        else:
            raise ValueError(f"Version information in {file_path} does not match expected format.")

    def _apply_file_mapping(
        self,
        input_df: pd.DataFrame,
        file_name: str,
        col_map: dict[int, str],
        join_key: str,
        target_col: str,
    ) -> pd.DataFrame:

        file_path = f"{self.dictionary_path}/{file_name}.csv"

        if os.path.exists(file_path):
            indices = list(col_map.keys())
            col_names = list(col_map.values())

            df = pd.read_csv(
                file_path,
                header=None,
                dtype=str,
                usecols=indices,
                names=col_names,
            )

            df[join_key] = df[join_key].str.strip()

            return pd.merge(input_df, df, on=join_key, how="left")

        final_df = input_df.copy()
        final_df[target_col] = None
        return final_df

    def process_data(self, metadata: WhoDrugVersionMetadata) -> pd.DataFrame:
        final_df = self._process_atc_data(metadata)
        return final_df

    def _process_atc_data(self, metadata: WhoDrugVersionMetadata) -> pd.DataFrame:
        """
        Reads the active ingredient data for C3 and B3 formats.
        Processes the ATC hierarchy levels into flattened level 4 codes.
        Merges them to include the format's primary identifier and drug name.
        """
        atc_file_name = (
            C3WHODrugFiles.ATC.value if metadata.format == WhoDrugFormats.C3.value else B3WHODrugFiles.INA.value
        )
        atc_file_path = f"{self.dictionary_path}/{atc_file_name}.csv"

        atc_df = pd.read_csv(atc_file_path, header=None, dtype=str, names=["atc_code", "level", "atc_text"])

        atc_df["atc_code"] = atc_df["atc_code"].str.strip()
        atc_df["atc_text"] = atc_df["atc_text"].str.strip()
        code_to_text_map = dict(zip(atc_df["atc_code"], atc_df["atc_text"]))
        level_4_codes = atc_df[atc_df["level"] == "4"]

        result_df = pd.DataFrame({"atc_code": level_4_codes["atc_code"]})
        level_to_length = {
            "level_1": 1,
            "level_2": 3,
            "level_3": 4,
            "level_4": 5,
        }

        for level, length in level_to_length.items():
            result_df[level] = result_df["atc_code"].apply(
                lambda x: code_to_text_map.get(x[:length], None) if len(x) >= length else None
            )

        if metadata.format == WhoDrugFormats.C3.value:
            med_prod_df = self._apply_file_mapping(
                input_df=result_df,
                file_name=C3WHODrugFiles.ThG.value,
                col_map={1: "atc_code", 4: "med_prod_id"},
                join_key="atc_code",
                target_col="med_prod_id",
            )
            final_df = self._apply_file_mapping(
                input_df=med_prod_df,
                file_name=C3WHODrugFiles.MP.value,
                col_map={0: "med_prod_id", 8: "drug_name"},
                join_key="med_prod_id",
                target_col="drug_name",
            )

        elif metadata.format == WhoDrugFormats.B3.value:
            drug_rec_df = self._apply_file_mapping(
                input_df=result_df,
                file_name=B3WHODrugFiles.DDA.value,
                col_map={0: "drug_rec_num", 4: "atc_code"},
                join_key="atc_code",
                target_col="drug_rec_num",
            )
            final_df = self._apply_file_mapping(
                input_df=drug_rec_df,
                file_name=B3WHODrugFiles.DD.value,
                col_map={0: "drug_rec_num", 11: "drug_name"},
                join_key="drug_rec_num",
                target_col="drug_name",
            )

        else:
            raise ValueError(f"Unsupported format: {metadata.format}")

        final_df = final_df.rename(columns={"atc_code": "term_code", "drug_name": "term_name"})
        return final_df
