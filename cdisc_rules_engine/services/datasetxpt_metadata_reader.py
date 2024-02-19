import pandas as pd
import pyreadstat
from xport import LOG as XPORT_LOG

from cdisc_rules_engine.services import logger
from cdisc_rules_engine.services.adam_variable_reader import AdamVariableReader


XPORT_LOG.disabled = True


class DatasetXPTMetadataReader:
    """
    Responsibility of the class is to read metadata
    from .xpt file.
    """

    # TODO. Maybe in future it is worth having multiple constructors
    #  like from_bytes, from_file etc. But now there is no immediate need for that.
    def __init__(self, file_path: str, file_name: str):
        # with open(file_path, "rb") as f:
        # self._file_contents = f.read()
        self.file_path = file_path
        self._metadata_container = None
        self._domain_name = None
        self._dataset_name = file_name.split(".")[0].upper()

    def read(self) -> dict:
        """
        Extracts metadata from binary contents of .xpt file.
        """
        chunk_size = 1000000
        dataset_row_count: int = 0
        df, meta = pyreadstat.read_xport(self.file_path, metadataonly=True)
        for df, _meta in pyreadstat.read_file_in_chunks(
            pyreadstat.read_xport,
            self.file_path,
            chunk_size,
            usecols=[list(meta.column_names)[0]],
        ):
            dataset_row_count += _meta.number_rows
            print(dataset_row_count)

        meta_Format = [
            "" if item == "NULL" else item + "."
            for item in list(meta.original_variable_types.values())
        ]
        self._domain_name = meta.table_name
        self._metadata_container = {
            "variable_labels": list(meta.column_labels),
            "variable_names": list(meta.column_names),
            "variable_formats": meta_Format,
            "variable_name_to_label_map": pd.Series(
                meta.column_labels, index=meta.column_names
            ).to_dict(),
            "variable_name_to_data_type_map": {
                key: "Char" if value == "string" else "Num"
                for key, value in meta.readstat_variable_types.items()
            },
            "variable_name_to_size_map": meta.variable_storage_width,
            "number_of_variables": len(meta.column_names),
            "dataset_label": meta.file_label,
            "dataset_length": dataset_row_count,
            "domain_name": self._domain_name,
            "dataset_name": self._dataset_name,
            "dataset_modification_date": meta.modification_time.isoformat(),
        }
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
