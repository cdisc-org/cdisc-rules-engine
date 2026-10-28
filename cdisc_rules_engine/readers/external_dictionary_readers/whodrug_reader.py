import csv
import os
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


FILE_EXTENSIONS = (".csv", ".txt")
DELIMITER_NAMES = {None: "fixed-width", ",": "comma-delimited", "\t": "tab-delimited"}

REQUIRED_FILES = {
    WhoDrugFormats.C3.value: (C3WHODrugFiles.ATC.value, C3WHODrugFiles.ThG.value, C3WHODrugFiles.MP.value),
    WhoDrugFormats.B3.value: (B3WHODrugFiles.INA.value, B3WHODrugFiles.DDA.value, B3WHODrugFiles.DD.value),
}

B3_FIXED_WIDTH_COLSPECS = {
    B3WHODrugFiles.INA.value: [(0, 7), (7, 8), (8, 118)],
    B3WHODrugFiles.DDA.value: [(0, 6), (6, 8), (8, 11), (11, 12), (12, 19)],
    B3WHODrugFiles.DD.value: [
        (0, 6),
        (6, 8),
        (8, 11),
        (11, 12),
        (12, 13),
        (13, 15),
        (15, 19),
        (19, 24),
        (24, 26),
        (26, 27),
        (27, 30),
        (30, 1530),
    ],
}


class WhoDrugReader:
    """Reader for the WHO Drug dictionary."""

    def __init__(self, pgi: PostgresQLInterface, dictionary_path: str):
        self.pgi = pgi
        self.dictionary_path = dictionary_path

    def _resolve_file(self, file_name: str) -> tuple[str, str | None] | None:
        files_by_lower_name = {name.lower(): name for name in os.listdir(self.dictionary_path)}
        for extension in FILE_EXTENSIONS:
            actual_name = files_by_lower_name.get(f"{file_name}{extension}".lower())
            if actual_name is None:
                continue
            file_path = f"{self.dictionary_path}/{actual_name}"
            with open(file_path, mode="r", encoding="utf-8-sig") as f:
                first_line = f.readline()
            if "\t" in first_line:
                return file_path, "\t"
            if extension == ".txt" and file_name in B3_FIXED_WIDTH_COLSPECS:
                first_field = (next(csv.reader([first_line])) or [""])[0]
                if len(first_field) > B3_FIXED_WIDTH_COLSPECS[file_name][0][1]:
                    return file_path, None
            return file_path, ","
        return None

    def _not_found_error(self, *file_names: str) -> FileNotFoundError:
        extensions = " or ".join(FILE_EXTENSIONS)
        return FileNotFoundError(
            f"WHODrug file(s) {', '.join(file_names)} not found in {self.dictionary_path} (looked for {extensions})"
        )

    def _parse_error(self, file_name: str, problem: str) -> ValueError:
        file_path, delimiter = self._resolve_file(file_name)
        return ValueError(f"{problem} in {file_path} (read as {DELIMITER_NAMES[delimiter]}); check the file format.")

    def _read_file(self, file_name: str, col_map: dict[int, str]) -> pd.DataFrame:
        resolved = self._resolve_file(file_name)
        if resolved is None:
            raise self._not_found_error(file_name)
        file_path, delimiter = resolved
        read_kwargs = {
            "header": None,
            "dtype": str,
            "usecols": list(col_map.keys()),
            "names": list(col_map.values()),
            "encoding": "utf-8-sig",
        }
        if delimiter is None:
            return pd.read_fwf(file_path, colspecs=B3_FIXED_WIDTH_COLSPECS[file_name], **read_kwargs)
        if delimiter == "\t":
            read_kwargs["quoting"] = csv.QUOTE_NONE
        return pd.read_csv(file_path, sep=delimiter, **read_kwargs)

    def _extract_version_metadata(self) -> WhoDrugVersionMetadata:
        """Extract metadata from the Version file."""
        resolved = self._resolve_file(UniversalWhoDrugFiles.VERSION.value)
        if resolved is None:
            raise self._not_found_error(UniversalWhoDrugFiles.VERSION.value)
        file_path, _ = resolved
        with open(file_path, mode="r", encoding="utf-8-sig") as f:
            version_info = f.readline().strip().strip('"')

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

        df = self._read_file(file_name, col_map)
        df[join_key] = df[join_key].str.strip()
        df = df.drop_duplicates()

        merged_df = pd.merge(input_df, df, on=join_key, how="left")
        if merged_df[target_col].isna().all():
            raise self._parse_error(file_name, f"No {target_col} values matched on {join_key}")
        return merged_df

    def process_data(self, metadata: WhoDrugVersionMetadata) -> pd.DataFrame:
        final_df = self._process_atc_data(metadata)
        return final_df

    def _process_atc_data(self, metadata: WhoDrugVersionMetadata) -> pd.DataFrame:
        """
        Reads the active ingredient data for C3 and B3 formats.
        Processes the ATC hierarchy levels into flattened level 4 codes.
        Merges them to include the format's primary identifier and drug name.
        """
        required_files = REQUIRED_FILES.get(metadata.format)
        if required_files is None:
            raise ValueError(f"Unsupported format: {metadata.format}")
        missing_files = [file_name for file_name in required_files if self._resolve_file(file_name) is None]
        if missing_files:
            raise self._not_found_error(*missing_files)

        atc_file_name = required_files[0]
        atc_df = self._read_file(atc_file_name, {0: "atc_code", 1: "level", 2: "atc_text"})

        atc_df["atc_code"] = atc_df["atc_code"].str.strip()
        atc_df["atc_text"] = atc_df["atc_text"].str.strip()
        code_to_text_map = dict(zip(atc_df["atc_code"], atc_df["atc_text"]))
        level_4_codes = atc_df[atc_df["level"] == "4"]
        if level_4_codes.empty:
            raise self._parse_error(atc_file_name, "No level 4 ATC codes found")

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

        else:
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

        final_df = final_df.rename(columns={"atc_code": "term_code", "drug_name": "term_name"})
        return final_df
