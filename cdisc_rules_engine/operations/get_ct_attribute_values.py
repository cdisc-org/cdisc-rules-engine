# python core.py test -s sdtmig -v 3-4 -dp ./dist/ts-test-dataset.json
# -r ./dist/rule-329.json -ct "sdtmct-2022-12-16"

# from typing import List

from cdisc_rules_engine.operations.base_operation import BaseOperation

# from collections import OrderedDict

# import pandas as pd


class CTAttributeValues(BaseOperation):
    def _execute_operation(self):
        """
        Fetches column order for a given domain from the CDISC library.
        Returns it as a Series of lists like:
        0    ["STUDYID", "DOMAIN", ...]
        1    ["STUDYID", "DOMAIN", ...]
        2    ["STUDYID", "DOMAIN", ...]
        ...

        Length of Series is equal to the length of given dataframe.
        The lists with column names are sorted
        in accordance to "ordinal" key of library metadata.
        """

        return self._get_ct_attribute_values()

    def _get_ct_attribute_values(self):

        ct_ver = "2020-03-27"
        sel_cols = ["TSVCDREF", "TSVCDVER", "TSVALCD"]
        # fn_ts = "./TS.xlsx"
        # df = pd.read_excel(fn_ts, sheet_name="TS")
        df = self.params.dataframe
        df_sel = df[(df["TSVCDVER"] == ct_ver)].loc[:, sel_cols]

        print(f"Data Selected: {df_sel}\n")

        print(f"Params: {self.params}\n")
        print(f"Original Dataset: {self.evaluation_dataset}\n")
        print(f"Cache Service: {self.cache}\n")
        print(f"Data Service: {self.data_service}\n")

        # print(f"Describe: {df.describe}")

        return df
