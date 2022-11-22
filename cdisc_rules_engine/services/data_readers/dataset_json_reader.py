import pandas as pd
import json

from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
)

# Testing only
# test_file_path = "D:\\CDISC_CORE_Engine\\Python_work_Jozef\\lb.json"


class DatasetJSONReader(DataReaderInterface):
    def from_file(self, file_path):
        # print("dataset_json_reader: starting reading_from_file = ", file_path)
        f = open(file_path)
        # returns JSON object as
        # a dictionary
        data = json.load(f)
        # print("data = ", data)
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
        # print("Creating DataFrame for dataset with ItemGroupOID = ", item_group_oid)
        # we need the column names
        # TODO: need also support for "referenceData" for trial design domains
        meta_data = data[start_data_string]["itemGroupData"][item_group_oid]["items"]
        column_names = []
        for x in meta_data:
            column_names.append(x["name"])
        # print("With column names = \n", column_names)
        # Set up an empty DataFrame, only containing the columns
        df = pd.DataFrame(columns=column_names)
        # then add row by row
        # See e.g. https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.from_dict.html
        # print("Retrieving data for ItemGroupOID = ", item_group_oid)
        for row in data[start_data_string]["itemGroupData"][item_group_oid]["itemData"]:
            # print(row)
            # df.append(test,ignore_index=False, verify_integrity=False, sort=None)
            df.loc[len(df)] = row
        # print("Final DataFrame = ", df)
        return df

    def read(self, data):
        pass


# Testing only
#reader1 = DatasetJSONReader()
#mydf = reader1.from_file("D:\\CDISC_CORE_Engine\\Python_work_Jozef\\lb.json")
