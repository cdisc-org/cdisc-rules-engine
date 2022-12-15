import pandas as pd
import json

from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)

# Testing only
# test_file_path = "D:\\CDISC_CORE_Engine\\Python_work_Jozef_OLD\\lb.json"


class DatasetJSONReader(DataReaderInterface):
    def from_file(self, file_path):
        # f = open(file_path)
        # returns JSON object as
        # a dictionary
        # data = json.load(f)
        # Replacement 2022-12-15: ensures that the file is closed once read
        with open(file_path, "r") as f:
            data = json.load(f)
        # check for "clinicalData". If not found, use "referenceData" (that is used for Trial Design datasets)
        if 'clinicalData' in data:
            start_data_string = "clinicalData"
        else:
            start_data_string = "referenceData"
        # we need to know the OID of the "ItemGroupData"
        # Probably, this can be done simpler ...
        item_group_data = data[start_data_string]["itemGroupData"]
        item_group_oids = item_group_data.keys()
        item_group_oid = list(item_group_oids)[0]
        # print("Creating DataFrame for dataset with ItemGroupOID = ", item_group_oid)
        # we need the column names
        meta_data = data[start_data_string]["itemGroupData"][item_group_oid]["items"]
        column_names = []
        for x in meta_data:
            column_names.append(x["name"])
        # Set up an empty DataFrame, only containing the columns
        df = pd.DataFrame(columns=column_names)
        # then add row by row
        # See e.g. https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.from_dict.html
        # print("Retrieving data for ItemGroupOID = ", item_group_oid)
        for row in data[start_data_string]["itemGroupData"][item_group_oid]["itemData"]:
            # df.append(test,ignore_index=False, verify_integrity=False, sort=None)
            df.loc[len(df)] = row
        print("Final DataFrame = ", df)
        return df

    def read(self, data):
        pass


# Testing only
# reader1 = DatasetJSONReader()
# mydf = reader1.from_file("D:\\CDISC_CORE_Engine\\Python_work_Jozef_OLD\\lb.json")
