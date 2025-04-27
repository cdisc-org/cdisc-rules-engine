import os
import json
import jsonschema
import pandas as pd


from cdisc_rules_engine.services import logger
from cdisc_rules_engine.services.adam_variable_reader import AdamVariableReader


class DatasetJSONMetadataReader:
    """
    Responsibility of the class is to read metadata
    from .json file.
    """

    def __init__(self, file_path: str, file_name: str):
        self._metadata_container = {}
        self._file_path = file_path
        self._first_record = None
        self._dataset_name = file_name.split(".")[0].upper()

    def read(self) -> dict:
        """
        Extracts metadata from .json file.
        """
        # Load Dataset-JSON Schema
        with open(
            os.path.join("resources", "schema", "dataset.schema.json")
        ) as schemajson:
            schema = schemajson.read()
        schema = json.loads(schema)

        with open(self._file_path, "r") as file:
            datasetjson = json.load(file)

        try:
            jsonschema.validate(datasetjson, schema)

            self._first_record = self._extract_first_record(datasetjson)
            self._metadata_container = {
                "variable_labels": [item["label"] for item in datasetjson["columns"]],
                "variable_names": [item["name"] for item in datasetjson["columns"]],
                "variable_formats": [
                    item.get("displayFormat", "") for item in datasetjson["columns"]
                ],
                "variable_name_to_label_map": {
                    item["name"]: item["label"] for item in datasetjson["columns"]
                },
                "variable_name_to_data_type_map": {
                    item["name"]: item["dataType"] for item in datasetjson["columns"]
                },
                "variable_name_to_size_map": {
                    item["name"]: item.get("length", None)
                    for item in datasetjson["columns"]
                },
                "number_of_variables": len(datasetjson["columns"]),
                "dataset_label": datasetjson.get("label"),
                "dataset_length": datasetjson.get("records"),
                "first_record": self._first_record,
                "dataset_name": datasetjson.get("name"),
                "dataset_modification_date": datasetjson["datasetJSONCreationDateTime"],
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
                f"{str(self._file_path)} is not compliant with Dataset-JSON schema"
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

    def _extract_first_record(self, datasetjson):
        try:
            return {
                name: value.decode("utf-8") if isinstance(value, bytes) else str(value)
                for name, value in pd.DataFrame(
                    [datasetjson.get("rows", [])[0]] if datasetjson.get("rows") else [],
                    columns=[col["name"] for col in datasetjson.get("columns", [])],
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
