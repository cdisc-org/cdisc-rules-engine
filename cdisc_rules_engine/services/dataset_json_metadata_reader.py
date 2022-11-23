import pandas as pd
from pandas import DataFrame
import json


class DatasetJSONMetadataReader:
    """
    Responsibility of the class is to read metadata
    from Dataset-JSON file.
    """

    def __init__(self, file_path: str):
        self._file_path = file_path
        self._metadata_container = None

    def read(self) -> dict:
        """
        Extracts metadata from contents of Dataset-JSON file.
        """
        # 2022-11-18: For the moment, we need "variable_labels", "variable_names"
        # later, we may need "variable_name_to_label_map" and "variable_name_to_data_type_map"
        # and maybe (???) a few others
        # print("dataset_json_metadata_reader: starting reading_from_file = ", self._file_path)
        f = open(self._file_path)
        # returns JSON object as
        # a dictionary
        data = json.load(f)
        # check for "clinicalData"
        start_data_string = "clinicalData"
        if 'clinicalData' in data:
            start_data_string = "clinicalData"
        else:
            # print("could not find clinicalData key in file - trying referenceData")
            start_data_string = "referenceData"
        # we need to know the OID of the "ItemGroupData"
        # TODO: very probably, this can be much simpler ...
        item_group_data = data[start_data_string]["itemGroupData"]
        item_group_oids = item_group_data.keys()
        item_group_oid = list(item_group_oids)[0]
        dataset_name = data[start_data_string]["itemGroupData"][item_group_oid]["name"]
        dataset_label = data[start_data_string]["itemGroupData"][item_group_oid]["label"]
        # TODO: fill variable_names and variable_labels
        variable_names = list()
        variable_labels = list()
        print("DatasetJSONMetadataReader: dataset name = ", dataset_name)
        print("DatasetJSONMetadataReader: dataset label = ", dataset_label)
        self._metadata_container = {
            # "variable_labels": list(dataset.contents.Label.values),
            "variable_labels": list,
            # "variable_names": list(dataset.contents.Variable.values),
            "variable_names": list,
            # "variable_name_to_label_map": pd.Series(
            #    dataset.contents.Label.values, index=dataset.contents.Variable
            # ).to_dict(),
            "variable_name_to_label_map": list,
            # "variable_name_to_data_type_map": pd.Series(
            #    dataset.contents.Type.values, index=dataset.contents.Variable
            # ).to_dict(),
            "variable_name_to_data_type_map": list,
            # "variable_name_to_size_map": pd.Series(
            #    dataset.contents.Length.values, index=dataset.contents.Variable
            # ).to_dict(),
            "variable_name_to_size_map": list,
            "domain_name": dataset_name,
            "dataset_label": dataset_label,
            "dataset_name": dataset_name,
        }
        return self._metadata_container

    # TODO: copied from dataset_metadata_reader.py - Is this needed?
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

# Testing only
# reader1 = DatasetJSONMetadataReader("D:\\CDISC_CORE_Engine\\Python_work_Jozef\\lb.json")
# mydf = reader1.read()
# print("mydf = ", mydf)
