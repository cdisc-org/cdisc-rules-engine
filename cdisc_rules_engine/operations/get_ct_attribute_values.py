from cdisc_rules_engine.operations.base_operation import BaseOperation
import pandas as pd


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
        # 1.0 get input variables
        # -------------------------------------------------------------------
        cc = "CT_PACKAGE"
        cv = self.params.ct_attribute

        # 2.0 build codelist from cache
        # -------------------------------------------------------------------
        ct_cache = self._get_ct_from_cache(ct_key=cc, ct_val=cv)

        # 3.0 get dataset records
        # -------------------------------------------------------------------
        ct_data = self._get_ct_from_dataset(ct_key=cc, ct_val=cv)

        # 4.0 merge the two datasets by CC
        # -------------------------------------------------------------------
        cc_key = ct_data[cc].to_list()
        ct_list = ct_cache[(ct_cache[cc].isin(cc_key))]
        n = self.params.dataframe.shape[0]
        result = pd.Series([ct_list[cv].values[0] for _ in range(n)])
        return result

    def _get_ct_from_cache(self, ct_key: str, ct_val: str):
        ct_packages = self.params.ct_package
        ct_term_maps = (
            []
            if ct_packages is None
            else [self.cache.get(package) or {} for package in ct_packages]
        )

        # convert codelist to dataframe
        ct_result = {ct_key: [], ct_val: []}

        for item in ct_term_maps:
            ct_result[ct_key].append(item["package"])
            codes = sorted(set(code for code in item.keys() if code != "package"))
            ct_result[ct_val].append(codes)

        return pd.DataFrame(ct_result)

    def _get_ct_from_dataset(self, ct_key: str, ct_val: str):
        ct_packages = self.params.ct_package
        ct_attr = self.params.ct_attribute
        c1 = self.params.target
        c2 = self.params.ct_version
        cc = ct_key
        ct_cd = ct_attr
        if ct_attr == "Term CCODE":
            ct_cd = "TSVALCD"
        sel_cols = [c1, c2, ct_cd, cc]

        # get dataframe from dataset records
        df = self.params.dataframe

        # add CT_PACKAGE column
        df[cc] = df.apply(
            lambda row: "sdtmct-" + row[c2]
            if row[c1] is not None and row[c1] in ("CDISC", "CDISC CT")
            else row[c1] + "-" + row[c2],
            axis=1,
        )

        # select records
        df_sel = df[(df[cc].isin(ct_packages))].loc[:, sel_cols]

        # group the records
        result = df_sel.groupby(cc)[ct_cd].unique().reset_index()
        result.rename(columns={ct_cd: ct_val})

        return result
