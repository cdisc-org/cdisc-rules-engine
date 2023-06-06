#
# python core.py test -s sdtmig -v 3-4 -dp ./dist/ts-test-dataset.json
# -r ./dist/rule-329.json -ct "sdtmct-2022-12-16" -ct "sdtmct-2022-09-30"
# core -> test --> test_rule (from scripts.test_rule import test as test_rule) ->
# validate_single_rule (scripts.test_rule) -> engine.test_validation (RuleEngine) ->
# validate_single_rule -> validate_rule (rule_engine) ->
# execute_rule (rule_engine) -> rule_processor.perform_rule_operations
#
# added code in
#   1. rules_engine.validate_rule
#   2. rule_processor.perform_rule_operations
#   3. added ct_package: list = None in rules_engine.execute_rule
#   4. added ct_package=ct_package for perform_rule_operations
#

# from typing import List
# import json
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

        ct_attr = self.params.ct_attribute
        ct_packages = self.params.ct_package
        ct_versions = [item["package"] for item in ct_packages]
        print(f"CT versions: {ct_versions}\n")
        ct_keys = ct_packages[0].keys()
        print(f"CT Keys: {ct_keys}")

        # r_ct = json.dumps(ct_packages, indent=4)
        # print(f"CT Packages: {r_ct}\n")

        ct_cd = None
        if ct_attr == "Term CCODE":
            ct_cd = "TSVALCD"

        c1 = self.params.target
        c2 = self.params.ct_version
        cc = "CT_PACKAGE"

        sel_cols = [c1, c2, ct_cd, cc]
        df = self.params.dataframe
        # add CT_PACKAGE column
        c1 = sel_cols[0]
        c2 = sel_cols[1]
        df[cc] = df.apply(
            lambda row: "sdtmct-" + row[c2]
            if row[c1] is not None and row[c1] == "CDISC"
            else row[c1] + "-" + row[c2],
            axis=1,
        )
        # print(f"Describe: {df.describe}")

        df_sel = df[(df[cc].isin(ct_versions))].loc[:, sel_cols]
        print(f"Data Selected: {df_sel}\n")

        df_grp = df_sel.groupby(cc)[ct_cd].unique().reset_index()

        print(f"Data Grouped: {df_grp}\n")

        # print(f"Params: {self.params}\n")
        # print(f"Original Dataset: {self.evaluation_dataset}\n")
        print(f"Cache Service: {self.cache}\n")
        print(f"Data Service: {self.data_service}\n")

        # print(f"Describe: {df.describe}")

        return df_grp
