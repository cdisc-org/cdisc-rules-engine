import pandas as pd
import xport.v56
from xport import LOG as XPORT_LOG

from cdisc_rules_engine.services import logger
from cdisc_rules_engine.services.adam_variable_reader import AdamVariableReader

xport.v56.LOG.disabled = True
XPORT_LOG.disabled = True


class DatasetMetadataReader:
    """
    Responsibility of the class is to read metadata
    from .xpt file.
    """

    # TODO. Maybe in future it is worth having multiple constructors
    #  like from_bytes, from_file etc. But now there is no immediate need for that.
    def __init__(self, contents_in_bytes: bytes):
        self._file_contents = contents_in_bytes
        self._metadata_container = None
        self._domain_name = None

    def read(self) -> dict:
        """
        Extracts metadata from binary contents of .xpt file.
        """
        dataset_container = xport.v56.loads(self._file_contents)
        dataset_id = next(iter(dataset_container))
        dataset = dataset_container.get(dataset_id)
        self._domain_name = self._extract_domain_name(dataset)
        self._metadata_container = {
            "variable_labels": list(dataset.contents.Label.values),
            "variable_names": list(dataset.contents.Variable.values),
            "variable_name_to_label_map": pd.Series(
                dataset.contents.Label.values, index=dataset.contents.Variable
            ).to_dict(),
            "variable_name_to_data_type_map": pd.Series(
                dataset.contents.Type.values, index=dataset.contents.Variable
            ).to_dict(),
            "variable_name_to_size_map": pd.Series(
                dataset.contents.Length.values, index=dataset.contents.Variable
            ).to_dict(),
            "number_of_variables": len(dataset.columns),
            "dataset_label": dataset.dataset_label,
            "domain_name": self._domain_name,
            "dataset_name": dataset.name,
            "dataset_modification_date": dataset.modified.isoformat(),
        }
        self._domain_name = self._extract_domain_name(dataset)
        self._convert_variable_types()
        self._metadata_container["adam_info"] = self._extract_adam_info(
            self._metadata_container["variable_names"]
        )
        logger.info(f"Extracted dataset metadata. metadata={self._metadata_container}")
        return self._metadata_container

    def _extract_domain_name(self, df):
        first_row = df.iloc[0]
        if "DOMAIN" in first_row:
            domain_name = first_row["DOMAIN"]
            if isinstance(domain_name, bytes):
                return domain_name.decode("utf-8")
            else:
                return str(domain_name)
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
            "variable_names": self._metadata_container.column_names,
            "variable_name_to_label_map": self._metadata_container.column_names_to_labels,
            "variable_name_to_data_type_map": self._metadata_container.readstat_variable_types,
            "variable_name_to_size_map": self._metadata_container.variable_storage_width,
            "number_of_variables": self._metadata_container.number_columns,
            "dataset_label": self._metadata_container.file_label,
            "domain_name": self._domain_name,
            "dataset_name": self._metadata_container.table_name,
            "dataset_modification_date": self._metadata_container.dataset_modification_date,
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
