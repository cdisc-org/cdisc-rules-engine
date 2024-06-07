import pyreadstat

from cdisc_rules_engine.services import logger
from cdisc_rules_engine.config import config
from cdisc_rules_engine.services.adam_variable_reader import AdamVariableReader
import os


class DatasetXPTMetadataReader:
    """
    Responsibility of the class is to read metadata
    from .xpt file.
    """

    # TODO. Maybe in future it is worth having multiple constructors
    #  like from_bytes, from_file etc. But now there is no immediate need for that.
    def __init__(self, file_path: str, file_name: str):
        file_size = os.path.getsize(file_path)
        if file_size > config.get_dataset_size_threshold():
            self._estimate_dataset_length = True
            self.row_limit = 1
        else:
            self._estimate_dataset_length = False
            self.row_limit = 0
        self._metadata_container = {}
        self._domain_name = None
        self._dataset_name = file_name.split(".")[0].upper()
        self._file_path = file_path

    def read(self) -> dict:
        """
        Extracts metadata from binary contents of .xpt file.
        """
        dataset, metadata = pyreadstat.read_xport(
            self._file_path, row_limit=self.row_limit
        )
        self._domain_name = self._extract_domain_name(dataset)
        self._metadata_container = {
            "variable_labels": list(metadata.column_labels),
            "variable_names": list(metadata.column_names),
            "variable_formats": [
                "" if data_type == "NULL" else data_type
                for data_type in metadata.original_variable_types.values()
            ],
            "variable_name_to_label_map": metadata.column_names_to_labels,
            "variable_name_to_data_type_map": metadata.readstat_variable_types,
            "variable_name_to_size_map": metadata.variable_storage_width,
            "number_of_variables": metadata.number_columns,
            "dataset_label": metadata.file_label,
            "dataset_length": metadata.number_rows,
            "domain_name": self._domain_name,
            "dataset_name": self._dataset_name,
            "dataset_modification_date": metadata.modification_time.isoformat(),
        }

        if self._estimate_dataset_length:
            self._metadata_container[
                "dataset_length"
            ] = self._calculate_dataset_length()
        self._domain_name = self._extract_domain_name(dataset)
        self._convert_variable_types()
        self._metadata_container["adam_info"] = self._extract_adam_info(
            self._metadata_container["variable_names"]
        )
        logger.info(f"Extracted dataset metadata. metadata={self._metadata_container}")
        return self._metadata_container

    def _extract_domain_name(self, df):
        try:
            first_row = df.iloc[0]
            if "DOMAIN" in first_row:
                domain_name = first_row["DOMAIN"]
                if isinstance(domain_name, bytes):
                    return domain_name.decode("utf-8")
                else:
                    return str(domain_name)
        except IndexError:
            pass
        return None

    def _calculate_dataset_length(self):
        df, meta = pyreadstat.read_xport(self._file_path, metadataonly=True)
        row_size = sum(meta.variable_storage_width.values())
        total_size = os.path.getsize(self._file_path)
        start = self._read_header(self._file_path)
        remainder = (total_size - start) % row_size
        estimated_rows = (total_size - start - remainder) / row_size
        if row_size < 80:
            padding = self._count_trailing_padding(self._file_path, row_size)
            return (total_size - start - padding) / row_size
        return estimated_rows

    def _count_trailing_padding(self, file_path, row_size):
        """
        reads the file from the end in chunks of 80 bytes and counts the total number of trailing padding bytes
        """
        padding_size = 0
        with open(file_path, "rb") as file:
            file.seek(0, os.SEEK_END)
            file_size = file.tell()
            file.seek(file_size - 1)

            while file.tell() > 0:
                byte = file.read(1)
                if byte == b" ":
                    padding_size += 1
                    if file.tell() == 1:
                        break
                    file.seek(file.tell() - 2)
                else:
                    break
        return padding_size

    def _read_header(self, file_path):
        """
        read the header of the file to find the start of the data section in 10kb chunks
        """
        marker = b"HEADER RECORD*******OBS     HEADER RECORD!!!!!!!000000000000000000000000000000"
        chunk_size = 1024
        read_header = b""
        padding_chars = (b"\x00", b" ")
        with open(file_path, "rb") as file:
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                read_header += chunk
                position = read_header.find(marker)
                if position != -1:
                    data_start = position + len(marker)
                    for i in range(data_start, len(read_header)):
                        if read_header[i : i + 1] not in padding_chars:
                            return i
        raise ValueError("End descriptor not found in the file header.")

    def _convert_variable_types(self):
        """
        Converts variable types to the format that
        rule authors use.
        """
        rule_author_type_map: dict = {
            "string": "Char",
            "double": "Num",
            "Character": "Char",
            "Numeric": "Num",
        }
        for key, value in self._metadata_container[
            "variable_name_to_data_type_map"
        ].items():
            self._metadata_container["variable_name_to_data_type_map"][
                key
            ] = rule_author_type_map[value]

    def _to_dict(self) -> dict:
        """
        This method is used to transform metadata_container
        object into dictionary.
        """
        return {
            "variable_labels": self._metadata_container.column_labels,
            "variable_formats": self._metadata_container.column_formats,
            "variable_names": self._metadata_container.column_names,
            "variable_name_to_label_map": self._metadata_container.column_names_to_labels,  # noqa
            "variable_name_to_data_type_map": self._metadata_container.readstat_variable_types,  # noqa
            "variable_name_to_size_map": self._metadata_container.variable_storage_width,  # noqa
            "number_of_variables": self._metadata_container.number_columns,
            "dataset_label": self._metadata_container.file_label,
            "domain_name": self._domain_name,
            "dataset_name": self._dataset_name,
            "dataset_modification_date": self._metadata_container.dataset_modification_date,  # noqa
        }

    def _extract_adam_info(self, variable_names):
        ad = AdamVariableReader()
        adam_columns = ad.extract_columns(variable_names)
        for column in adam_columns:
            ad.check_y(column)
            ad.check_w(column)
            ad.check_xx_zz(column)
        adam_info_dict = {
            "categorization_scheme": ad.categorization_scheme,
            "w_indexes": ad.w_indexes,
            "period": ad.period,
            "selection_algorithm": ad.selection_algorithm,
        }
        return adam_info_dict
