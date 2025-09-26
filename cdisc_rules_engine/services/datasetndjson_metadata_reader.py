import os
import json
import jsonschema
import pandas as pd


from cdisc_rules_engine.services import logger
from cdisc_rules_engine.services.adam_variable_reader import AdamVariableReader
from cdisc_rules_engine.services.data_readers.json_reader import JSONReader


class DatasetNDJSONMetadataReader:
    """
    Responsibility of the class is to read metadata
    from .ndjson file.
    """

    def __init__(self, file_path: str, file_name: str):
        self._metadata_container = {}
        self._file_path = file_path
        self._first_record = None
        self._dataset_name = file_name.split(".")[0].upper()

    def read(self) -> dict:
        """
        Extracts metadata from .ndjson file.
        """
        # Load Dataset-NDJSON Schema
        schema = JSONReader().from_file(
            os.path.join("resources", "schema", "dataset-ndjson-schema.json")
        )

        with open(self._file_path, "r") as file:
            lines = file.readlines()

        metadatandjson = json.loads(lines[0])

        datandjson = json.loads(lines[1]) if len(lines) > 1 else []

        try:

            jsonschema.validate(metadatandjson, schema)

            self._first_record = self._extract_first_record(metadatandjson, datandjson)
            self._metadata_container = {
                "variable_labels": [
                    item["label"] for item in metadatandjson["columns"]
                ],
                "variable_names": [item["name"] for item in metadatandjson["columns"]],
                "variable_formats": [
                    item.get("displayFormat", "") for item in metadatandjson["columns"]
                ],
                "variable_name_to_label_map": {
                    item["name"]: item["label"] for item in metadatandjson["columns"]
                },
                "variable_name_to_data_type_map": {
                    item["name"]: item["dataType"] for item in metadatandjson["columns"]
                },
                "variable_name_to_size_map": {
                    item["name"]: item.get("length", None)
                    for item in metadatandjson["columns"]
                },
                "number_of_variables": len(metadatandjson["columns"]),
                "dataset_label": metadatandjson.get("label"),
                "dataset_length": metadatandjson.get("records"),
                "first_record": self._first_record,
                "dataset_name": metadatandjson.get("name"),
                "dataset_modification_date": metadatandjson[
                    "datasetJSONCreationDateTime"
                ],
            }

            self._convert_variable_types()

            self._metadata_container["adam_info"] = self._extract_adam_info(
                self._metadata_container["variable_names"]
            )
            logger.info(
                f"Extracted dataset metadata. metadata={self._metadata_container}"
            )

            return self._metadata_container

        except jsonschema.exceptions.ValidationError:
            logger.warning(
                f"{str(self._file_path)} is not compliant with Dataset-NDJSON schema"
            )
            return {
                "variable_labels": [],
                "variable_names": [],
                "variable_formats": [],
                "variable_name_to_label_map": {},
                "variable_name_to_data_type_map": {},
                "variable_name_to_size_map": {},
                "number_of_variables": 0,
                "dataset_label": "",
                "dataset_length": 0,
                "first_record": {},
                "dataset_name": "",
                "dataset_modification_date": "",
            }

    def _extract_first_record(self, metadatandjson, datandjson):
        try:
            return {
                name: value.decode("utf-8") if isinstance(value, bytes) else str(value)
                for name, value in pd.DataFrame(
                    [
                        dict(
                            zip(
                                [
                                    col["name"]
                                    for col in metadatandjson.get("columns", [])
                                ],
                                datandjson,
                            )
                        )
                    ]
                )
                .iloc[0]
                .items()
            }
        except IndexError:
            pass
        return None

    def _convert_variable_types(self):
        """
        Converts variable types to the format that
        rule authors use.
        """
        rule_author_type_map: dict = {
            "boolean": "Num",
            "decimal": "Num",
            "double": "Num",
            "float": "Num",
            "integer": "Num",
            "string": "Char",
            "datetime": "Char",
            "date": "Char",
            "time": "Char",
            "URI": "Char",
        }
        for key, value in self._metadata_container[
            "variable_name_to_data_type_map"
        ].items():
            self._metadata_container["variable_name_to_data_type_map"][key] = (
                rule_author_type_map[value]
            )

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
            "first_record": self._first_record,
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
